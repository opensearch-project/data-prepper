/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.otellogs.http;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import org.opensearch.dataprepper.exceptions.BadRequestException;
import org.opensearch.dataprepper.exceptions.BufferWriteException;
import org.opensearch.dataprepper.logging.DataPrepperMarkers;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.log.OpenTelemetryLog;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.otel.codec.OTelOutputFormat;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCodec;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoOpensearchCodec;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoStandardCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.ConsumesJson;
import com.linecorp.armeria.server.annotation.ConsumesProtobuf;
import com.linecorp.armeria.server.annotation.Post;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;

public class ArmeriaHttpService {
    private static final Logger LOG = LoggerFactory.getLogger(ArmeriaHttpService.class);

    public static final String REQUESTS_RECEIVED = "requestsReceived";
    public static final String SUCCESS_REQUESTS = "successRequests";
    public static final String PAYLOAD_SIZE = "payloadSize";
    public static final String REQUEST_PROCESS_DURATION = "requestProcessDuration";

    private final OTelProtoCodec.OTelProtoDecoder oTelProtoDecoder;
    private final Buffer<Record<Object>> buffer;

    private final int bufferWriteTimeoutInMillis;

    private final Counter requestsReceivedCounter;
    private final Counter successRequestsCounter;
    private final DistributionSummary payloadSizeSummary;
    private final Timer requestProcessDuration;

    public ArmeriaHttpService(
            Buffer<Record<Object>> buffer,
            final PluginMetrics pluginMetrics,
            final int bufferWriteTimeoutInMillis,
            final OTelOutputFormat otelOutputFormat
    ) {
        this.buffer = buffer;
        this.oTelProtoDecoder = (otelOutputFormat == OTelOutputFormat.OPENSEARCH) ? new OTelProtoOpensearchCodec.OTelProtoDecoder() : new OTelProtoStandardCodec.OTelProtoDecoder();
        this.bufferWriteTimeoutInMillis = bufferWriteTimeoutInMillis;

        requestsReceivedCounter = pluginMetrics.counter(REQUESTS_RECEIVED);
        successRequestsCounter = pluginMetrics.counter(SUCCESS_REQUESTS);
        payloadSizeSummary = pluginMetrics.summary(PAYLOAD_SIZE);
        requestProcessDuration = pluginMetrics.timer(REQUEST_PROCESS_DURATION);
    }

    // no path provided. Will be set by config.
    @Post("")
    @ConsumesJson
    @ConsumesProtobuf
    public ExportLogsServiceResponse exportLog(ExportLogsServiceRequest request) {
        requestsReceivedCounter.increment();
        payloadSizeSummary.record(request.getSerializedSize());

        requestProcessDuration.record(() -> processRequest(request));

        return ExportLogsServiceResponse.newBuilder().build();
    }

    private void processRequest(final ExportLogsServiceRequest request) {
        final List<OpenTelemetryLog> logs;

        try {
            logs = oTelProtoDecoder.parseExportLogsServiceRequest(request, Instant.now());
        } catch (Exception e) {
            LOG.warn(DataPrepperMarkers.SENSITIVE, "Failed to parse the request with error {}. Request body: {}", e, request);
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
    }
}
