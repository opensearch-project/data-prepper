/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.source.oteltrace;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.buffer.SizeOverflowException;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.model.trace.Span;
import com.amazon.dataprepper.plugins.source.oteltrace.codec.OTelProtoCodec;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class OTelTraceGrpcService extends TraceServiceGrpc.TraceServiceImplBase {
    private static final Logger LOG = LoggerFactory.getLogger(OTelTraceGrpcService.class);

    public static final String REQUEST_TIMEOUTS = "requestTimeouts";
    public static final String REQUESTS_RECEIVED = "requestsReceived";

    private final int bufferWriteTimeoutInMillis;
    private final Buffer<Record<Span>> buffer;

    private final Counter requestTimeoutCounter;
    private final Counter requestsReceivedCounter;


    public OTelTraceGrpcService(int bufferWriteTimeoutInMillis,
                                Buffer<Record<Span>> buffer,
                                final PluginMetrics pluginMetrics) {
        this.bufferWriteTimeoutInMillis = bufferWriteTimeoutInMillis;
        this.buffer = buffer;

        requestTimeoutCounter = pluginMetrics.counter(REQUEST_TIMEOUTS);
        requestsReceivedCounter = pluginMetrics.counter(REQUESTS_RECEIVED);
    }


    @Override
    public void export(ExportTraceServiceRequest request, StreamObserver<ExportTraceServiceResponse> responseObserver) {
        requestsReceivedCounter.increment();

        if (Context.current().isCancelled()) {
            requestTimeoutCounter.increment();
            responseObserver.onError(Status.CANCELLED.withDescription("Cancelled by client").asRuntimeException());
            return;
        }

        Collection<Span> spans = new ArrayList<>();

        try {
            spans = OTelProtoCodec.parseExportTraceServiceRequest(request);
        } catch (Exception e) {
            LOG.error("Failed to parse the request content [{}] due to:", request, e);
        }

        final List<Record<Span>> records = spans.stream()
                .map(Record::new)
                .collect(Collectors.toList());

        try {
            buffer.writeAll(records, bufferWriteTimeoutInMillis);
            responseObserver.onNext(ExportTraceServiceResponse.newBuilder().build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            LOG.error("Failed to write the request content [{}] due to:", request, e);
            if (e instanceof TimeoutException) {
                requestTimeoutCounter.increment();
                responseObserver
                        .onError(Status.RESOURCE_EXHAUSTED.withDescription(e.getMessage())
                                .asException());
            } else if (e instanceof SizeOverflowException) {
                responseObserver
                        .onError(Status.RESOURCE_EXHAUSTED.withDescription(e.getMessage())
                                .asException());
            } else {
                responseObserver
                        .onError(Status.INTERNAL.withDescription(e.getMessage())
                                .asException());
            }
        }
    }
}
