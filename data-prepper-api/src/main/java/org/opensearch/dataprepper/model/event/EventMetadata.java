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
     * Returns the attributes
     * @return a map of attributes
     * @since 1.2
     */
    Map<String, Object> getAttributes();

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
     * Adds a tag to the Metadata
     * @param tag to be added
     * @since 2.3
     */
    void addTag(final String tag);
}
