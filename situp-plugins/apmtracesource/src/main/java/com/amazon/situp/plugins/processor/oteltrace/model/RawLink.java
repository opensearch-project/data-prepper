package com.amazon.situp.plugins.processor.oteltrace.model;

import io.opentelemetry.proto.trace.v1.Span;
import org.apache.commons.codec.binary.Hex;

import java.util.Map;

/**
 * Java POJO of https://github.com/open-telemetry/opentelemetry-proto/blob/master/opentelemetry/proto/trace/v1/trace.proto#L196
 * which is compatible with elasticsearch
 */
public final class RawLink {

    /**
     * HexString representation of the trace_id in the @see <a href="https://github.com/open-telemetry/opentelemetry-proto/blob/master/opentelemetry/proto/trace/v1/trace.proto#L199">OpenTelemetry spec</a>
     */
    private final String traceId;
    /**
     * HexString representation of the span_id in the @see <a href="https://github.com/open-telemetry/opentelemetry-proto/blob/master/opentelemetry/proto/trace/v1/trace.proto#L202">OpenTelemetry spec</a>
     */
    private final String spanId;

    private final String traceState;
    private final Map<String, Object> attributes;

    private final int droppedAttributesCount;

    public String getTraceId() {
        return traceId;
    }

    public String getSpanId() {
        return spanId;
    }

    public String getTraceState() {
        return traceState;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public int getDroppedAttributesCount() {
        return droppedAttributesCount;
    }

    private RawLink(String traceId, String spanId, String traceState, Map<String, Object> attributes, int droppedAttributesCount) {
        this.traceId = traceId;
        this.spanId = spanId;
        this.traceState = traceState;
        this.attributes = attributes;
        this.droppedAttributesCount = droppedAttributesCount;
    }

    public static RawLink buildRawLink(final Span.Link link) {
        return new RawLink(Hex.encodeHexString(link.getTraceId().toByteArray()),
                Hex.encodeHexString(link.getSpanId().toByteArray()),
                link.getTraceState(),
                OTelProtoHelper.getLinkAttributes(link),
                link.getDroppedAttributesCount());
    }
}
