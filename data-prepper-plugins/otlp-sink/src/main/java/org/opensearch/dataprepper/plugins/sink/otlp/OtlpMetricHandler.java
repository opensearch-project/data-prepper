/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.otlp;

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoStandardCodec;
import software.amazon.awssdk.utils.Pair;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Handler for OTLP metric signals.
 */
public class OtlpMetricHandler implements OtlpSignalHandler {
    private final OTelProtoStandardCodec.OTelProtoEncoder encoder;

    public OtlpMetricHandler(final OTelProtoStandardCodec.OTelProtoEncoder encoder) {
        this.encoder = encoder;
    }

    @Override
    public Object encodeEvent(final Event event) throws Exception {
        return encoder.convertToResourceMetrics((Metric) event);
    }

    @Override
    public long getSerializedSize(final Object encodedData) {
        return ((ResourceMetrics) encodedData).getSerializedSize();
    }

    @Override
    public byte[] buildRequestPayload(final List<Pair<Object, EventHandle>> batch) {
        final ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder()
                .addAllResourceMetrics(batch.stream()
                        .map(p -> (ResourceMetrics) p.left())
                        .collect(Collectors.toList()))
                .build();
        return request.toByteArray();
    }

    @Override
    public Pair<Long, String> parsePartialSuccess(final byte[] responseBytes) throws Exception {
        final ExportMetricsServiceResponse response = ExportMetricsServiceResponse.parseFrom(responseBytes);
        if (response.hasPartialSuccess()) {
            final long rejectedCount = response.getPartialSuccess().getRejectedDataPoints();
            final String errorMessage = response.getPartialSuccess().getErrorMessage();
            return Pair.of(rejectedCount, errorMessage);
        }
        return Pair.of(0L, "");
    }
}
