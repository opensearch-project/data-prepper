/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.otellogs;

import com.linecorp.armeria.server.ServiceRequestContext;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import org.opensearch.dataprepper.exceptions.BadRequestException;
import org.opensearch.dataprepper.exceptions.BufferWriteException;
import org.opensearch.dataprepper.exceptions.RequestCancelledException;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.log.OpenTelemetryLog;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class OTelLogsGrpcService extends LogsServiceGrpc.LogsServiceImplBase {
    private static final Logger LOG = LoggerFactory.getLogger(OTelLogsGrpcService.class);

    public static final String REQUESTS_RECEIVED = "requestsReceived";
    public static final String SUCCESS_REQUESTS = "successRequests";
    public static final String PAYLOAD_SIZE = "payloadSize";
    public static final String REQUEST_PROCESS_DURATION = "requestProcessDuration";
    public static final String REQUEST_PARSING_DURATION = "requestParsingDuration";

    private final int bufferWriteTimeoutInMillis;

    private final OTelProtoCodec.OTelProtoDecoder oTelProtoDecoder;

    private final Buffer<Record<Object>> buffer;

    private final Counter requestsReceivedCounter;

    private final Counter successRequestsCounter;
    private final DistributionSummary payloadSizeSummary;
    private final Timer requestProcessDuration;
    private final Timer requestParsingDuration;

    public OTelLogsGrpcService(int bufferWriteTimeoutInMillis,
                               final OTelProtoCodec.OTelProtoDecoder oTelProtoDecoder,
                               final Buffer<Record<Object>> buffer,
                               final PluginMetrics pluginMetrics,
                               final String metricsPrefix) {
        this.bufferWriteTimeoutInMillis = bufferWriteTimeoutInMillis;
        this.buffer = buffer;

        if (metricsPrefix != null) {
            requestsReceivedCounter = pluginMetrics.counter(REQUESTS_RECEIVED, metricsPrefix);
            successRequestsCounter = pluginMetrics.counter(SUCCESS_REQUESTS, metricsPrefix);
            payloadSizeSummary = pluginMetrics.summary(PAYLOAD_SIZE, metricsPrefix);
            requestProcessDuration = pluginMetrics.timer(REQUEST_PROCESS_DURATION, metricsPrefix);
            requestParsingDuration = pluginMetrics.timer(REQUEST_PARSING_DURATION, metricsPrefix);

        } else {
            requestsReceivedCounter = pluginMetrics.counter(REQUESTS_RECEIVED);
            successRequestsCounter = pluginMetrics.counter(SUCCESS_REQUESTS);
            payloadSizeSummary = pluginMetrics.summary(PAYLOAD_SIZE);
            requestProcessDuration = pluginMetrics.timer(REQUEST_PROCESS_DURATION);
            requestParsingDuration = pluginMetrics.timer(REQUEST_PARSING_DURATION);
        }

        this.oTelProtoDecoder = oTelProtoDecoder;
    }

    @Override
    public void export(ExportLogsServiceRequest request, StreamObserver<ExportLogsServiceResponse> responseObserver) {
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

    private void processRequest(final ExportLogsServiceRequest request, final StreamObserver<ExportLogsServiceResponse> responseObserver) {
        final List<OpenTelemetryLog> logs;

        try {
            final long startParsingTime = System.currentTimeMillis();
            logs = oTelProtoDecoder.parseExportLogsServiceRequest(request, Instant.now());
            requestParsingDuration.record(System.currentTimeMillis() - startParsingTime, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            LOG.error("Failed to parse the request {} due to:", request, e);
            throw new BadRequestException(e.getMessage(), e);
        }

        try {
            if (buffer.isByteBuffer()) {
                buffer.writeBytes(request.toByteArray(), null, bufferWriteTimeoutInMillis);
            } else {
                final List<Record<Object>> records = logs.stream().map(log -> new Record<Object>(log)).collect(Collectors.toList());
                buffer.writeAll(records, bufferWriteTimeoutInMillis);
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
        responseObserver.onNext(ExportLogsServiceResponse.newBuilder().build());
        responseObserver.onCompleted();
    }
}
