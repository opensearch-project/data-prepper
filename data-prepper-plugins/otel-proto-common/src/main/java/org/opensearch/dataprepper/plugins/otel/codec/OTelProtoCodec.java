/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.otel.codec;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import org.apache.commons.codec.DecoderException;

import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.log.OpenTelemetryLog;
import org.opensearch.dataprepper.model.trace.Span;

import java.io.UnsupportedEncodingException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * OTelProtoCodec is for encoding/decoding between {@link org.opensearch.dataprepper.model.trace} and {@link io.opentelemetry.proto}.
 */
public interface OTelProtoCodec {
    public static final int DEFAULT_EXPONENTIAL_HISTOGRAM_MAX_ALLOWED_SCALE = 10;

    public interface OTelProtoDecoder {

        List<Span> parseExportTraceServiceRequest(final ExportTraceServiceRequest exportTraceServiceRequest, final Instant timeReceived);

        Map<String, ExportTraceServiceRequest> splitExportTraceServiceRequestByTraceId(final ExportTraceServiceRequest exportTraceServiceRequest);

        List<OpenTelemetryLog> parseExportLogsServiceRequest(final ExportLogsServiceRequest exportLogsServiceRequest, final Instant timeReceived);
        default Collection<Record<? extends Metric>> parseExportMetricsServiceRequest(
                            final ExportMetricsServiceRequest request,
                            final Instant timeReceived) {
            AtomicInteger droppedCounter = new AtomicInteger(0);
            return parseExportMetricsServiceRequest(request, droppedCounter,
                    DEFAULT_EXPONENTIAL_HISTOGRAM_MAX_ALLOWED_SCALE, timeReceived, true, true, true);
        }
        Collection<Record<? extends Metric>> parseExportMetricsServiceRequest(
                            final ExportMetricsServiceRequest request,
                            AtomicInteger droppedCounter,
                            final Integer exponentialHistogramMaxAllowedScale,
                            final Instant timeReceived,
                            final boolean calculateHistogramBuckets,
                            final boolean calculateExponentialHistogramBuckets,
                            final boolean flattenAttributes);
    }

    public interface OTelProtoEncoder {
        ResourceSpans convertToResourceSpans(final Span span) throws UnsupportedEncodingException, DecoderException;
    }

}
