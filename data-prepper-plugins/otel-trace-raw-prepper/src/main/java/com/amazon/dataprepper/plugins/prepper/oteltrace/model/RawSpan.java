package com.amazon.dataprepper.plugins.prepper.oteltrace.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;


public final class RawSpan {
    private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    /**
     * HexString representation of the trace_id in the @see <a href="https://github.com/open-telemetry/opentelemetry-proto/blob/master/opentelemetry/proto/trace/v1/trace.proto#L75">OpenTelemetry spec</a>
     */
    private final String traceId;
    /**
     * HexString representation of the span_id in the @see <a href="https://github.com/open-telemetry/opentelemetry-proto/blob/master/opentelemetry/proto/trace/v1/trace.proto#L75">OpenTelemetry spec</a>
     */
    private final String spanId;
    /**
     * trace_state in the @see <a href="https://github.com/open-telemetry/opentelemetry-proto/blob/master/opentelemetry/proto/trace/v1/trace.proto#L80">OpenTelemetry spec</a>
     * This is w3c information field set when different vendors are used.
     */
    private final String traceState;
    /**
     * HexString representation of the parent_span_id in the @see <a href="https://github.com/open-telemetry/opentelemetry-proto/blob/master/opentelemetry/proto/trace/v1/trace.proto#L84">OpenTelemetry spec</a>
     */
    private final String parentSpanId;
    /**
     * String description of the span's operation.
     * name in the the see <a href="https://github.com/open-telemetry/opentelemetry-proto/blob/master/opentelemetry/proto/trace/v1/trace.proto#L99">OpenTelemetry spec</a>
     */
    private final String name;
    /**
     * String representation of the span context in the  <a href="https://github.com/open-telemetry/opentelemetry-proto/blob/master/opentelemetry/proto/trace/v1/trace.proto#L134">OpenTelemetry spec</a>
     */
    private final String kind;
    /**
     * ISO8601 representation of the start_time_in_unix_nano in the <a href="https://github.com/open-telemetry/opentelemetry-proto/blob/master/opentelemetry/proto/trace/v1/trace.proto#L142">OpenTelemetry spec</a>
     */
    private final String startTime;
    /**
     * ISO8601 representation of the end_time_in_unix_nano in the <a href="https://github.com/open-telemetry/opentelemetry-proto/blob/master/opentelemetry/proto/trace/v1/trace.proto#L142">OpenTelemetry spec</a>
     */
    private final String endTime;
    /**
     * Duration is calculated from startTime and endTime in nanos. This is a  friendly field to make trace analytics kibana UI.
     */
    private final long durationInNanos;
    /**
     * Extract the serviceName from the span.
     */
    private final String serviceName;
    /**
     * Collection of key-value pairs related to the span.attributes, resource.attributes, span.status and instrumentationLibrary
     * This is done to avoid nested objects and support kibana query and aggregations.
     */
    private final Map<String, Object> attributes;
    /**
     * Collection of events related to the span.
     * events field in the <a href=https://github.com/open-telemetry/opentelemetry-proto/blob/master/opentelemetry/proto/trace/v1/trace.proto#L186>OpenTelemetry spec</a>
     */
    private final List<RawEvent> events;
    /**
     * Collection of links related to the span.
     * links field in the <a href=https://github.com/open-telemetry/opentelemetry-proto/blob/master/opentelemetry/proto/trace/v1/trace.proto#L186>OpenTelemetry spec</a>
     */
    private final List<RawLink> links;

    /**
     * dropped_attributes_count in the <a href=https://github.com/open-telemetry/opentelemetry-proto/blob/master/opentelemetry/proto/trace/v1/trace.proto#L165>OpenTelemetry Spec</a>
     */
    private final int droppedAttributesCount;

    /**
     * dropped_events_count in the <a href=https://github.com/open-telemetry/opentelemetry-proto/blob/master/opentelemetry/proto/trace/v1/trace.proto#L190>OpenTelemetry Spec</a>
     */
    private final int droppedEventsCount;

    /**
     * dropped_links_count in the <a href=https://github.com/open-telemetry/opentelemetry-proto/blob/master/opentelemetry/proto/trace/v1/trace.proto#L221>OpenTelemetry Spec</a>
     */
    private final int droppedLinksCount;

    /**
     * Trace group represent root span i.e the span which triggers the trace event with the microservice architecture. This field is not part of the OpenTelemetry Spec.
     * This field is something specific to Trace Analytics feature that Kibana will be supporting. This field is derived from the opentelemetry spec and set as below,
     * <p>
     * if (this.parentSpanId.isEmpty()) {
     * traceGroup = this.name;
     * }
     */
    private TraceGroup traceGroup;


    public String getTraceId() {
        return traceId;
    }

    public String getSpanId() {
        return spanId;
    }

    public String getParentSpanId() {
        return parentSpanId;
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getStartTime() {
        return startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public String getName() {
        return name;
    }

    public String getTraceState() {
        return traceState;
    }

    public String getKind() {
        return kind;
    }

    public long getDurationInNanos() {
        return durationInNanos;
    }

    public List<RawEvent> getEvents() {
        return events;
    }

    public List<RawLink> getLinks() {
        return links;
    }

    public int getDroppedAttributesCount() {
        return droppedAttributesCount;
    }

    public int getDroppedEventsCount() {
        return droppedEventsCount;
    }

    public int getDroppedLinksCount() {
        return droppedLinksCount;
    }

    public void setTraceGroup(TraceGroup traceGroup) {
        this.traceGroup = traceGroup;
    }

    RawSpan(RawSpanBuilder builder) {
        this.traceId = builder.traceId;
        this.spanId = builder.spanId;
        this.traceState = builder.traceState;
        this.parentSpanId = builder.parentSpanId;
        this.kind = builder.kind;
        this.name = builder.name;
        this.startTime = builder.startTime;
        this.endTime = builder.endTime;
        this.durationInNanos = builder.durationInNanos;
        this.serviceName = builder.serviceName;
        this.attributes = builder.attributes;
        this.events = builder.events;
        this.links = builder.links;
        this.droppedAttributesCount = builder.droppedAttributesCount;
        this.droppedEventsCount = builder.droppedEventsCount;
        this.droppedLinksCount = builder.droppedLinksCount;
        this.traceGroup = builder.traceGroup;
    }

    @JsonAnyGetter
    public Map<String, Object> getAttributes() {
        return attributes;
    }

    @JsonUnwrapped(prefix="traceGroup.")
    public TraceGroup getTraceGroup() {
        return traceGroup;
    }

    public String toJson() throws JsonProcessingException {
        return OBJECT_MAPPER.writeValueAsString(this);
    }
}
