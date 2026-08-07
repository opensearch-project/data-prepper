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

import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoStandardCodec;
import software.amazon.awssdk.utils.Pair;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Handler for OTLP trace signals.
 */
public class OtlpTraceHandler implements OtlpSignalHandler<ResourceSpans> {
    private final OTelProtoStandardCodec.OTelProtoEncoder encoder;

    public OtlpTraceHandler(final OTelProtoStandardCodec.OTelProtoEncoder encoder) {
        this.encoder = encoder;
    }

    @Override
    public ResourceSpans encodeEvent(final Event event) throws Exception {
        if (!(event instanceof Span)) {
            throw new IllegalArgumentException("OtlpTraceHandler expects a Span event but received: " + event.getClass().getName());
        }
        return encoder.convertToResourceSpans((Span) event);
    }

    @Override
    public long getSerializedSize(final ResourceSpans encodedData) {
        return encodedData.getSerializedSize();
    }

    @Override
    public byte[] buildRequestPayload(final List<Pair<ResourceSpans, EventHandle>> batch) {
        final ExportTraceServiceRequest request = ExportTraceServiceRequest.newBuilder()
                .addAllResourceSpans(batch.stream()
                        .map(Pair::left)
                        .collect(Collectors.toList()))
                .build();
        return request.toByteArray();
    }

    @Override
    public Pair<Long, String> parsePartialSuccess(final byte[] responseBytes) throws Exception {
        final ExportTraceServiceResponse response = ExportTraceServiceResponse.parseFrom(responseBytes);
        if (response.hasPartialSuccess()) {
            final long rejectedCount = response.getPartialSuccess().getRejectedSpans();
            final String errorMessage = response.getPartialSuccess().getErrorMessage();
            return Pair.of(rejectedCount, errorMessage);
        }
        return Pair.of(0L, "");
    }
}
