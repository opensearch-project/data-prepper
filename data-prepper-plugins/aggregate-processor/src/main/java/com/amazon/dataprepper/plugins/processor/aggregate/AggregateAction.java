/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.aggregate;

import com.amazon.dataprepper.model.event.Event;

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
     * @param aggregateActionInput An implementation of {@link com.amazon.dataprepper.plugins.processor.aggregate.AggregateActionInput}.
     *                             This AggregateActionInput exposes the {@link com.amazon.dataprepper.plugins.processor.aggregate.GroupState}
     *                             that is shared between all events in a single group. This GroupState is non-null.
     * @return An {@link com.amazon.dataprepper.plugins.processor.aggregate.AggregateActionResponse} with an Event that will either
     * be processed immediately, or is empty if the Event should be removed from processing
     * @since 1.3
     */
    default AggregateActionResponse handleEvent(final Event event, final AggregateActionInput aggregateActionInput) {
        return AggregateActionResponse.fromEvent(event);
    }

    /**
     * Concludes a group of Events
     *
     * @param aggregateActionInput The {@link com.amazon.dataprepper.plugins.processor.aggregate.AggregateActionInput} from previous calls to handleEvent
     * @return The final Event to return. Return empty if the aggregate processor
     * should not pass an event
     * @since 1.3
     */
    default Optional<Event> concludeGroup(final AggregateActionInput aggregateActionInput) {
        return Optional.empty();
    }
}
