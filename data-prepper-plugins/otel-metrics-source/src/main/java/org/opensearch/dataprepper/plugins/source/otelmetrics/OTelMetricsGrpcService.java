/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.otelmetrics;

import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

public class OTelMetricsGrpcService extends MetricsServiceGrpc.MetricsServiceImplBase {
    private static final Logger LOG = LoggerFactory.getLogger(OTelMetricsGrpcService.class);

    public static final String REQUEST_TIMEOUTS = "requestTimeouts";
    public static final String REQUESTS_RECEIVED = "requestsReceived";
    public static final String SUCCESS_REQUESTS = "successRequests";
    public static final String PAYLOAD_SIZE = "payloadSize";
    public static final String REQUEST_PROCESS_DURATION = "requestProcessDuration";

    private final int bufferWriteTimeoutInMillis;
    private final Buffer<Record<ExportMetricsServiceRequest>> buffer;

    private final Counter requestTimeoutCounter;
    private final Counter requestsReceivedCounter;
    private final Counter successRequestsCounter;
    private final DistributionSummary payloadSizeSummary;
    private final Timer requestProcessDuration;


    public OTelMetricsGrpcService(int bufferWriteTimeoutInMillis,
                                  Buffer<Record<ExportMetricsServiceRequest>> buffer,
                                  final PluginMetrics pluginMetrics) {
        this.bufferWriteTimeoutInMillis = bufferWriteTimeoutInMillis;
        this.buffer = buffer;

        requestTimeoutCounter = pluginMetrics.counter(REQUEST_TIMEOUTS);
        requestsReceivedCounter = pluginMetrics.counter(REQUESTS_RECEIVED);
        successRequestsCounter = pluginMetrics.counter(SUCCESS_REQUESTS);
        payloadSizeSummary = pluginMetrics.summary(PAYLOAD_SIZE);
        requestProcessDuration = pluginMetrics.timer(REQUEST_PROCESS_DURATION);
    }


    @Override
    public void export(final ExportMetricsServiceRequest request, final StreamObserver<ExportMetricsServiceResponse> responseObserver) {
        requestProcessDuration.record(() -> processRequest(request, responseObserver));
    }

    private void processRequest(final ExportMetricsServiceRequest request, final StreamObserver<ExportMetricsServiceResponse> responseObserver) {
        requestsReceivedCounter.increment();
        payloadSizeSummary.record(request.getSerializedSize());

        if (Context.current().isCancelled()) {
            requestTimeoutCounter.increment();
            responseObserver.onError(Status.CANCELLED.withDescription("Cancelled by client").asRuntimeException());
            return;
        }

        try {
            buffer.write(new Record<>(request), bufferWriteTimeoutInMillis);
            successRequestsCounter.increment();
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
