/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.otellogs;

import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

public class OTelLogsGrpcService extends LogsServiceGrpc.LogsServiceImplBase {
    private static final Logger LOG = LoggerFactory.getLogger(OTelLogsGrpcService.class);

    public static final String REQUEST_TIMEOUTS = "requestTimeouts";
    public static final String REQUESTS_RECEIVED = "requestsReceived";

    private final int bufferWriteTimeoutInMillis;
    private final Buffer<Record<ExportLogsServiceRequest>> buffer;

    private final Counter requestTimeoutCounter;
    private final Counter requestsReceivedCounter;


    public OTelLogsGrpcService(int bufferWriteTimeoutInMillis,
                               Buffer<Record<ExportLogsServiceRequest>> buffer,
                               final PluginMetrics pluginMetrics) {
        this.bufferWriteTimeoutInMillis = bufferWriteTimeoutInMillis;
        this.buffer = buffer;

        requestTimeoutCounter = pluginMetrics.counter(REQUEST_TIMEOUTS);
        requestsReceivedCounter = pluginMetrics.counter(REQUESTS_RECEIVED);
    }


    @Override
    public void export(ExportLogsServiceRequest request, StreamObserver<ExportLogsServiceResponse> responseObserver) {
        requestsReceivedCounter.increment();

        if (Context.current().isCancelled()) {
            requestTimeoutCounter.increment();
            responseObserver.onError(Status.CANCELLED.withDescription("Cancelled by client").asRuntimeException());
            return;
        }

        try {
            buffer.write(new Record<>(request), bufferWriteTimeoutInMillis);
            responseObserver.onNext(ExportLogsServiceResponse.newBuilder().build());
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
