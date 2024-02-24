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
import org.opensearch.dataprepper.exceptions.BufferWriteException;
import org.opensearch.dataprepper.exceptions.RequestCancelledException;
import static org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCodec.DEFAULT_EXPONENTIAL_HISTOGRAM_MAX_ALLOWED_SCALE;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCodec;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.metric.Metric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

public class OTelMetricsGrpcService extends MetricsServiceGrpc.MetricsServiceImplBase {
    private static final Logger LOG = LoggerFactory.getLogger(OTelMetricsGrpcService.class);

    public static final String REQUESTS_RECEIVED = "requestsReceived";
    public static final String SUCCESS_REQUESTS = "successRequests";
    public static final String RECORDS_CREATED = "recordsCreated";
    public static final String RECORDS_DROPPED = "recordsDropped";
    public static final String PAYLOAD_SIZE = "payloadSize";
    public static final String REQUEST_PROCESS_DURATION = "requestProcessDuration";

    private final int bufferWriteTimeoutInMillis;
    private final OTelProtoCodec.OTelProtoDecoder oTelProtoDecoder;
    private final Buffer<Record<? extends Metric>> buffer;

    private final Counter requestsReceivedCounter;
    private final Counter successRequestsCounter;
    private final Counter recordsCreatedCounter;
    private final Counter recordsDroppedCounter;
    private final DistributionSummary payloadSizeSummary;
    private final Timer requestProcessDuration;


    public OTelMetricsGrpcService(int bufferWriteTimeoutInMillis,
                                  final OTelProtoCodec.OTelProtoDecoder oTelProtoDecoder,
                                  Buffer<Record<? extends Metric>> buffer,
                                  final PluginMetrics pluginMetrics) {
        this.bufferWriteTimeoutInMillis = bufferWriteTimeoutInMillis;
        this.buffer = buffer;

        requestsReceivedCounter = pluginMetrics.counter(REQUESTS_RECEIVED);
        successRequestsCounter = pluginMetrics.counter(SUCCESS_REQUESTS);
        recordsCreatedCounter = pluginMetrics.counter(RECORDS_CREATED);
        recordsDroppedCounter = pluginMetrics.counter(RECORDS_DROPPED);
        payloadSizeSummary = pluginMetrics.summary(PAYLOAD_SIZE);
        requestProcessDuration = pluginMetrics.timer(REQUEST_PROCESS_DURATION);
        this.oTelProtoDecoder = oTelProtoDecoder;
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
        try {
            if (buffer.isByteBuffer()) {
                buffer.writeBytes(request.toByteArray(), null, bufferWriteTimeoutInMillis);
            } else {
                Collection<Record<? extends Metric>> metrics;

                AtomicInteger droppedCounter = new AtomicInteger(0);
                metrics = oTelProtoDecoder.parseExportMetricsServiceRequest(request, droppedCounter, DEFAULT_EXPONENTIAL_HISTOGRAM_MAX_ALLOWED_SCALE, true, true, false);
                recordsDroppedCounter.increment(droppedCounter.get());
                recordsCreatedCounter.increment(metrics.size());
                buffer.writeAll(metrics, bufferWriteTimeoutInMillis);
            }
        } catch (Exception e) {
            if (ServiceRequestContext.current().isTimedOut()) {
                LOG.warn("Exception writing to buffer but request already timed out.", e);
                return;
            }

            LOG.error("Failed to write the request of size {} due to:", request.toString().length(), e);
            throw new BufferWriteException(e.getMessage(), e);
        }

        if (ServiceRequestContext.current().isTimedOut()) {
            LOG.warn("Buffer write completed successfully but request already timed out.");
            return;
        }

        successRequestsCounter.increment();
        responseObserver.onNext(ExportMetricsServiceResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
}
