/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.otelmetrics;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.record.Record;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

public class OTelMetricsGrpcService extends MetricsServiceGrpc.MetricsServiceImplBase {
    private static final Logger LOG = LoggerFactory.getLogger(OTelMetricsGrpcService.class);

    public static final String REQUEST_TIMEOUTS = "requestTimeouts";
    public static final String REQUESTS_RECEIVED = "requestsReceived";

    private final int bufferWriteTimeoutInMillis;
    private final Buffer<Record<ExportMetricsServiceRequest>> buffer;

    private final Counter requestTimeoutCounter;
    private final Counter requestsReceivedCounter;


    public OTelMetricsGrpcService(int bufferWriteTimeoutInMillis,
                                  Buffer<Record<ExportMetricsServiceRequest>> buffer,
                                  final PluginMetrics pluginMetrics) {
        this.bufferWriteTimeoutInMillis = bufferWriteTimeoutInMillis;
        this.buffer = buffer;

        requestTimeoutCounter = pluginMetrics.counter(REQUEST_TIMEOUTS);
        requestsReceivedCounter = pluginMetrics.counter(REQUESTS_RECEIVED);
    }


    @Override
    public void export(ExportMetricsServiceRequest request, StreamObserver<ExportMetricsServiceResponse> responseObserver) {
        requestsReceivedCounter.increment();

        if (Context.current().isCancelled()) {
            requestTimeoutCounter.increment();
            responseObserver.onError(Status.CANCELLED.withDescription("Cancelled by client").asRuntimeException());
            return;
        }

        try {
            buffer.write(new Record<>(request), bufferWriteTimeoutInMillis);
            responseObserver.onNext(ExportMetricsServiceResponse.newBuilder().build());
            responseObserver.onCompleted();
        } catch (TimeoutException e) {
            LOG.error("Buffer is full, unable to write");
            requestTimeoutCounter.increment();
            responseObserver
                    .onError(Status.RESOURCE_EXHAUSTED.withDescription("Buffer is full, request timed out.")
                            .asException());
        }
    }
}
