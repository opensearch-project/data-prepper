package com.amazon.dataprepper.model.span;

import java.util.Map;

/**
 * A pointer from the current span to another span in a trace.
 */
public interface Link {

    /**
     * Gets the trace id of the linked span. The linked span may or may not be in the same trace.
     * @return the trace id
     */
    String getTraceId();

    /**
     * Gets the span id of the linked span
     * @return the span id
     */
    String getSpanId();

    /**
     * Gets the associated trace state
     * @return the trace state
     */
    String getTraceState();

    /**
     * Gets the attributes associated with the link
     * @return a map of attributes
     */
    Map<String, Object> getAttributes();

    /**
     * Gets the number of attributes dropped from the link. 0 indicates no links were dropped.
     * @return the number of dropped attributes
     */
    Integer getDroppedAttributesCount();
}
