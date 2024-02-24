/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.otelmetrics;

import com.linecorp.armeria.server.ServiceRequestContext;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import org.opensearch.dataprepper.exceptions.RequestCancelledException;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

public class OTelMetricsGrpcService extends MetricsServiceGrpc.MetricsServiceImplBase {
    private static final Logger LOG = LoggerFactory.getLogger(OTelMetricsGrpcService.class);

    public static final String REQUESTS_RECEIVED = "requestsReceived";
    public static final String SUCCESS_REQUESTS = "successRequests";
    public static final String PAYLOAD_SIZE = "payloadSize";
    public static final String REQUEST_PROCESS_DURATION = "requestProcessDuration";

    private final int bufferWriteTimeoutInMillis;
    private final Consumer<ExportMetricsServiceRequest> consumer;

    private final Counter requestsReceivedCounter;
    private final Counter successRequestsCounter;
    private final DistributionSummary payloadSizeSummary;
    private final Timer requestProcessDuration;


    public OTelMetricsGrpcService(int bufferWriteTimeoutInMillis,

                                  final Consumer<ExportMetricsServiceRequest> consumer,
                                  final PluginMetrics pluginMetrics) {
        this.bufferWriteTimeoutInMillis = bufferWriteTimeoutInMillis;
        this.consumer = consumer;

        requestsReceivedCounter = pluginMetrics.counter(REQUESTS_RECEIVED);
        successRequestsCounter = pluginMetrics.counter(SUCCESS_REQUESTS);
        payloadSizeSummary = pluginMetrics.summary(PAYLOAD_SIZE);
        requestProcessDuration = pluginMetrics.timer(REQUEST_PROCESS_DURATION);
    }

    @Override
    public void export(final ExportMetricsServiceRequest request, final StreamObserver<ExportMetricsServiceResponse> responseObserver) {
        requestsReceivedCounter.increment();
        payloadSizeSummary.record(request.getSerializedSize());

        if (ServiceRequestContext.current().isTimedOut()) {
            return;
        }

        if (Context.current().isCancelled()) {
            throw new RequestCancelledException("Cancelled by client");
        }

        requestProcessDuration.record(() -> processRequest(request, responseObserver));
    }

    private void processRequest(final ExportMetricsServiceRequest request, final StreamObserver<ExportMetricsServiceResponse> responseObserver) {
        consumer.accept(request);

        if (ServiceRequestContext.current().isTimedOut()) {
            LOG.warn("Buffer write completed successfully but request already timed out.");
            return;
        }

        successRequestsCounter.increment();
        responseObserver.onNext(ExportMetricsServiceResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
}
