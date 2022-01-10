/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.aggregate;

import com.amazon.dataprepper.model.event.Event;

import java.util.Map;
import java.util.Optional;

/**
 * Interface for creating custom actions to be used with the {@link com.amazon.dataprepper.plugins.processor.aggregate.AggregateProcessor}.
 * @since 1.3
 */
public interface AggregateAction {
    /**
     * Handles an event as part of aggregation.
     *
     * @param event The current event
     * @param groupState An arbitrary map for the current group
     * @return An {@link com.amazon.dataprepper.plugins.processor.aggregate.AggregateActionResponse} with an Event that will either
     * be processed immediately, or is empty if the Event should be removed from processing
     * @since 1.3
     */
    default AggregateActionResponse handleEvent(Event event, Map<Object, Object> groupState) {
        return new AggregateActionResponse(Optional.of(event), false);
    }

    /**
     * Concludes a group of Events
     *
     * @param groupState The groupState map from previous calls to handleEvent
     * @return The final Event to return. Return empty if the aggregate processor
     * should not pass an event
     * @since 1.3
     */
    default Optional<Event> concludeGroup(Map<Object, Object> groupState) {
        return Optional.empty();
    }
}
