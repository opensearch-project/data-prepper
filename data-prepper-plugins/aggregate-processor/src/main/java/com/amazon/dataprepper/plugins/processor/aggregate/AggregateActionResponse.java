/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.aggregate;

import com.amazon.dataprepper.model.event.Event;

import java.util.Optional;

/**
 * Model class to be returned in {@link com.amazon.dataprepper.plugins.processor.aggregate.AggregateAction}. Contains both the Event to be processed and an option
 * to close the current window for {@link com.amazon.dataprepper.plugins.processor.aggregate.AggregateProcessor} immediately after an event is handled.
 * @since 1.3
 */
public class AggregateActionResponse {
    private Event event;

    public AggregateActionResponse(final Event event) {
        this.event = event;
    }

    public Optional<Event> getEvent() {
        return Optional.ofNullable(event);
    }

    /**
     * @return an AggregateActionResponse with a null Event
     * @since 1.3
     */
    public static AggregateActionResponse emptyEventResponse() {
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
