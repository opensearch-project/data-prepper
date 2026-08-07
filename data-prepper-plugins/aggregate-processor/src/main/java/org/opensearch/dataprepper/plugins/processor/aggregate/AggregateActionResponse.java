/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.processor.aggregate;

import org.opensearch.dataprepper.model.event.Event;

/**
 * Model class to be returned in {@link AggregateAction}. Contains the Event to be processed, which is null if no Event should be processed.
 * @since 1.3
 */
public class AggregateActionResponse {
    private final Event event;

    public AggregateActionResponse(final Event event) {
        this.event = event;
    }

    /**
     * @return the AggregateActionResponse Event. Can be null
     */
    public Event getEvent() {
        return event;
    }

    /**
     * @return an AggregateActionResponse with a null Event
     * @since 1.3
     */
    public static AggregateActionResponse nullEventResponse() {
        return new AggregateActionResponse(null);
    }

    /**
     *
     * @param event The event to be part of the returned AggregateActionResponse
     * @return an AggregateActionResponse with a non-null Event
     * @since 1.3
     */
    public static AggregateActionResponse fromEvent(final Event event) {
        return new AggregateActionResponse(event);
    }
}
