/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.trace;

import org.opensearch.dataprepper.model.event.Event;

import java.util.List;
import java.util.Map;

/**
 * A span event in Data Prepper represents a single operation in a trace. Multiple spans can represent a single trace or a service map.
 * @since 1.2
 */
public interface Span extends Event {

    /**
     * Gets an id for the trace as a HexString.
     * @return the trace id
     * @since 1.2
     */
    String getTraceId();

    /**
     * Gets the id for the span as a HexString.
     * @return the span id
     * @since 1.2
     */
    String getSpanId();

    /**
     * Gets the trace_state.
     * @return the trace state
     * @since 1.2
     */
    String getTraceState();

    /**
     * Gets the span id of the parent as a HexString.
     * @return the parent span id
     * @since 1.2
     */
    String getParentSpanId();

    /**
     * Gets a String description of the span's operation.
     * @return the name
     * @since 1.2
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
     * @since 1.2
     */
    String getStartTime();

    /**
     * Gets ISO8601 representation of the end time.
     * @return the end time
     * @since 1.2
     */
    String getEndTime();

    /**
     * Gets a collection of key-value pairs related to the span.
     * @return A map of attributes
     * @since 1.2
     */
    Map<String, Object> getAttributes();

    /**
     * Gets the total number of discarded attributes. 0 indicates no attributes were dropped.
     * @return the number of dropped attributes
     * @since 1.2
     */
    Integer getDroppedAttributesCount();

    /**
     * Gets a collection of {@link SpanEvent}s related to the span.
     * @return a List of {@link SpanEvent}s
     * @since 1.2
     */
    List<? extends SpanEvent> getEvents();

    /**
     * Gets the total number of discarded events. 0 indicates no events were dropped.
     * @return the number of dropped events
     */
    Integer getDroppedEventsCount();

    /**
     * Gets a collections of {@link Link}s, which are references from this span to a span in the same or different trace.
     * @return a List of {@link Link}s
     * @since 1.2
     */
    List<? extends Link> getLinks();

    /**
     * Gets the total number of dropped links. 0 indicates no links were dropped.
     * @return the number of dropped links
     * @since 1.2
     */
    Integer getDroppedLinksCount();

    /**
     * Gets the trace group's name.
     * @return a string representing the trace group
     * @since 1.2
     */
    String getTraceGroup();

    /**
     * Gets the duration of the span in nanoseconds.
     * @return the duration of the span in nanoseconds
     * @since 1.2
     */
    Long getDurationInNanos();

    /**
     * Gets the {@link org.opensearch.dataprepper.model.trace.TraceGroupFields} for this span.
     * @return traceGroupFields
     * @since 1.2
     */
    TraceGroupFields getTraceGroupFields();

    /**
     * Gets the serviceName of this span.
     * @return the ServiceName
     * @since 1.3
     */
    String getServiceName();

    /**
     * Sets the trace group's name for this span.
     * @param traceGroup trace group's name
     * @since 1.3
     */
    void setTraceGroup(String traceGroup);

    /**
     * Sets the {@link org.opensearch.dataprepper.model.trace.TraceGroupFields} for this span.
     * @param traceGroupFields trace group related fields
     * @since 1.3
     */
    void setTraceGroupFields(TraceGroupFields traceGroupFields);

    /**
     * Gets the scope of this log event.
     *
     * @return the scope
     * @since 2.11
     */
    Map<String, Object> getScope();

    /**
     * Gets the resource of this log event.
     *
     * @return the resource
     * @since 2.11
     */
    Map<String, Object> getResource();

    /**
     * Gets the status of this log event.
     *
     * @return the status
     * @since 2.11
     */
    Map<String, Object> getStatus();
}
