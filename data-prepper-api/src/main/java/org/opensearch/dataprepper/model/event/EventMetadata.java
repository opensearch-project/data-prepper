/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The event metadata contains internal event fields. These fields are used only within Data Prepper, and are not passed down to Sinks.
 * @since 1.2
 */
public interface EventMetadata extends Serializable {

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
     * Returns the external origination time of the event
     * @return the external origination time
     * @since 2.6
     */
    Instant getExternalOriginationTime();

    /**
     * Sets the external origination time of the event
     * @param externalOriginationTime the external origination time
     * @since 2.6
     */
    void setExternalOriginationTime(Instant externalOriginationTime);

    /**
     * Returns the attributes
     * @return a map of attributes
     * @since 1.2
     */
    Map<String, Object> getAttributes();

    /**
     * Returns value of an attribute
     * @param key metadata key
     * @return value of an attribute
     * @since 2.3
     */
    Object getAttribute(final String key);

    /**
     * Sets an attribute
     * @param key to be set
     * @param value to be set
     * @since 2.3
     */
    void setAttribute(String key, Object value);

    /**
     * Returns the tags
     * @return a set of tags
     * @since 2.3
     */
    Set<String> getTags();

    /**
     * Indicates if a tag is present
     * @param tags list of the tags to be looked up
     * @return true if all tags are present, false otherwise
     * @since 2.3
     */
    Boolean hasTags(final List<String> tags);

    /**
     * Adds a tags to the Metadata
     * @param tags to be added
     * @since 2.3
     */
    void addTags(final List<String> tags);
}
