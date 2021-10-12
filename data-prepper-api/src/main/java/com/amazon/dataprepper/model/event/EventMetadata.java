package com.amazon.dataprepper.model.event;

import java.time.Instant;
import java.util.Map;

/**
 * The event metadata contains internal event fields. These fields are used only within Data Prepper, and are not passed down to Sinks.
 * @since 1.2
 */
public interface EventMetadata {

    /**
     * Retrieves the type of event
     * @return the event type
     * @since 1.2
     */
    String getEventType();

    /**
     * Returns the time the event was received in as an {@link Instant}
     * @return the time received
     * @since 1.2
     */
    Instant getTimeReceived();

    /**
     * Returns the attributes
     * @return a map of attributes
     * @since 1.2
     */
    Map<String, Object> getAttributes();
}
