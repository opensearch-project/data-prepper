package com.amazon.dataprepper.model.span;

import java.util.Map;

/**
 * A timestamped annotation of associated attributes for a span.
 */
public interface SpanEvent {

    /**
     * Gets the name of the event
     * @return the name
     */
    String getName();

    /**
     * Gets the time the event occurred.
     * @return the time
     */
    String getTime();

    /**
     * Gets a map of user-supplied attributes.
     * @return a map of attributes
     */
    Map<String, Object> getAttributes();

    /**
     * Gets the number of dropped attributes. 0 indicates no attributes were dropped.
     * @return the number of dropped attributes
     */
    Integer getDroppedAttributesCount();
}
