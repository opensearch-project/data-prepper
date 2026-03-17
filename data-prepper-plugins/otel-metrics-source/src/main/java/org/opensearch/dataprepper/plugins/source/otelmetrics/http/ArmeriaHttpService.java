/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.otelmetrics.http;

import java.time.Instant;
import java.util.Collection;

import org.opensearch.dataprepper.exceptions.BadRequestException;
import org.opensearch.dataprepper.exceptions.BufferWriteException;
import org.opensearch.dataprepper.logging.DataPrepperMarkers;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.ConsumesJson;
import com.linecorp.armeria.server.annotation.ConsumesProtobuf;
import com.linecorp.armeria.server.annotation.Post;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;

public class ArmeriaHttpService {
    private static final Logger LOG = LoggerFactory.getLogger(ArmeriaHttpService.class);

    public static final String REQUESTS_RECEIVED = "requestsReceived";
    public static final String SUCCESS_REQUESTS = "successRequests";
    public static final String PAYLOAD_SIZE = "payloadSize";
    public static final String REQUEST_PROCESS_DURATION = "requestProcessDuration";

    private final OTelProtoCodec.OTelProtoDecoder oTelProtoDecoder;
    private final Buffer<Record<? extends Metric>> buffer;

    private final int bufferWriteTimeoutInMillis;

    private final Counter requestsReceivedCounter;
    private final Counter successRequestsCounter;
    private final DistributionSummary payloadSizeSummary;
    private final Timer requestProcessDuration;

    public ArmeriaHttpService(Buffer<Record<? extends Metric>> buffer,
                              final PluginMetrics pluginMetrics,
                              final int bufferWriteTimeoutInMillis,
                              final OTelProtoCodec.OTelProtoDecoder oTelProtoDecoder ) {
        this.buffer = buffer;
        this.oTelProtoDecoder = oTelProtoDecoder;
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
    public ExportMetricsServiceResponse exportMetrics(ExportMetricsServiceRequest request) {
        requestsReceivedCounter.increment();
        payloadSizeSummary.record(request.getSerializedSize());

        requestProcessDuration.record(() -> processRequest(request));

        return ExportMetricsServiceResponse.newBuilder().build();
    }

    private void processRequest(final ExportMetricsServiceRequest request) {
        final Collection<Record<? extends Metric>> metrics;

        try {
            metrics = oTelProtoDecoder.parseExportMetricsServiceRequest(request, Instant.now());
        } catch (Exception e) {
            LOG.warn(DataPrepperMarkers.SENSITIVE, "Failed to parse the request with error {}. Request body: {}", e, request);
            throw new BadRequestException(e.getMessage(), e);
        }

        try {
            if (buffer.isByteBuffer()) {
                buffer.writeBytes(request.toByteArray(), null, bufferWriteTimeoutInMillis);
            } else {
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
    }
}
