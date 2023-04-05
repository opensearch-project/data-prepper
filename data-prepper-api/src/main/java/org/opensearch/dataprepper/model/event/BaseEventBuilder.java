/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.event;

import java.time.Instant;
import java.util.Map;

public interface BaseEventBuilder<T extends Event> {
    /**
     * Sets the event type for the metadata if a {@link #withEventMetadata} is not used.
     *
     * @param eventType the event type
     * @return returns the base event builder
     * @since 2.2
     */
    BaseEventBuilder<T> withEventType(final String eventType);

    /**
     * Sets the attributes for the metadata if a {@link #withEventMetadata} is not used.
     *
     * @param eventMetadataAttributes the attributes
     * @return returns the base event builder
     * @since 2.2
     */
    BaseEventBuilder<T> withEventMetadataAttributes(final Map<String, Object> eventMetadataAttributes);

    /**
     * Sets the time received for the metadata if a {@link #withEventMetadata} is not used.
     *
     * @param timeReceived the time an event was received
     * @return returns the base event builder
     * @since 2.2
     */
    BaseEventBuilder<T> withTimeReceived(final Instant timeReceived);

    /**
     * Sets the metadata.
     *
     * @param eventMetadata the metadata
     * @return returns the base event builder
     * @since 2.2
     */
    BaseEventBuilder<T> withEventMetadata(final EventMetadata eventMetadata);

    /**
     * Sets the data of the event.
     *
     * @param data the data
     * @return returns the base event builder
     * @since 2.2
     */
    BaseEventBuilder<T> withData(final Object data);
}
