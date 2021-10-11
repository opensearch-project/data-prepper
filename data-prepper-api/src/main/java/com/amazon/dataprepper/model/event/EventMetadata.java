package com.amazon.dataprepper.model.event;

import java.util.Map;

/**
 * The event metadata contains internal event fields. These fields are used only within Data Prepper, and are not passed down to Sinks.
 */
public interface EventMetadata {

    /**
     * Retrieves the type of event
     * @return the event type
     */
    String getEventType();

    /**
     * Returns the time the event was received in nanos
     * @return the time received
     */
    Long getTimeReceivedInNanos();

    /**
     * Returns the attributes
     * @return a map of attributes
     */
    Map<String, Object> getAttributes();
}
