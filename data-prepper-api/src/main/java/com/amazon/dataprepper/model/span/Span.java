package com.amazon.dataprepper.model.span;

import com.amazon.dataprepper.model.event.Event;

import java.util.List;
import java.util.Map;

/**
 * A span event in Data Prepper represents a single operation in a trace. Multiple spans can represent a single trace or a service map.
 */
public interface Span extends Event {

    /**
     * Gets an id for the trace as a HexString.
     * @return the trace id
     */
    String getTraceId();

    /**
     * Gets the id for the span as a HexString.
     * @return the span id
     */
    String getSpanId();

    /**
     * Gets the trace_state.
     * @return the trace state
     */
    String getTraceState();

    /**
     * Gets the span id of the parent as a HexString.
     * @return the parent span id
     */
    String getParentSpanId();

    /**
     * Gets a String description of the span's operation.
     * @return the name
     */
    String getName();

    /**
     * Gets the string representation of the span type.
     * @return the kind
     */
    String getKind();

    /**
     * Gets ISO8601 representation of the start time.
     * @return the start time
     */
    String getStartTime();

    /**
     * Gets ISO8601 representation of the end time.
     * @return the end time
     */
    String getEndTime();

    /**
     * Gets a collection of key-value pairs related to the span.
     * @return A map of attributes
     */
    Map<String, Object> getAttributes();

    /**
     * Gets the total number of discarded attributes. 0 indicates no attributes were dropped.
     * @return the number of dropped attributes
     */
    Integer getDroppedAttributesCount();

    /**
     * Gets a collection of {@link SpanEvent}s related to the span.
     * @return a List of {@link SpanEvent}s
     */
    List<SpanEvent> getEvents();

    /**
     * Gets the total number of discarded events. 0 indicates no events were dropped.
     * @return the number of dropped events
     */
    Integer getDroppedEventsCount();

    /**
     * Gets a collections of {@link Link}s, which are references from this span to a span in the same or different trace.
     * @return a List of {@link Link}s
     */
    List<Link> getLinks();

    /**
     * Gets the total number of dropped links. 0 indicates no links were dropped.
     * @return the number of dropped links
     */
    Integer getDroppedLinksCount();

    /**
     * Gets the trace group's name.
     * @return a string representing the trace group
     */
    String getTraceGroup();

    /**
     * Gets the duration of the span in nanoseconds.
     * @return the duration of the span in nanoseconds
     */
    Integer getDurationInNanos();

    /**
     * Gets the {@link TraceGroupFields} for this span.
     * @return traceGroupFields
     */
    TraceGroupFields getTraceGroupFields();
}
