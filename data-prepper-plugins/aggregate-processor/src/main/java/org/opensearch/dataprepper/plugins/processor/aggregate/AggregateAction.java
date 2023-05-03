/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate;

import org.opensearch.dataprepper.model.event.Event;

import java.util.List;

/**
 * Interface for creating custom actions to be used with the {@link AggregateProcessor}.
 * @since 1.3
 */
public interface AggregateAction {
    /**
     * Handles an event as part of aggregation.
     *
     * @param event The current event
     * @param aggregateActionInput An implementation of {@link AggregateActionInput}.
     *                             This AggregateActionInput exposes the {@link GroupState}
     *                             that is shared between all events in a single group. This GroupState is non-null.
     * @return An {@link AggregateActionResponse} with an Event that will either
     * be processed immediately, or is empty if the Event should be removed from processing
     * @since 1.3
     */
    default AggregateActionResponse handleEvent(final Event event, final AggregateActionInput aggregateActionInput) {
        return AggregateActionResponse.fromEvent(event);
    }

    /**
     * Concludes a group of Events
     *
     * @param aggregateActionInput The {@link AggregateActionInput} from previous calls to handleEvent
     * @return The final Event to return. Return empty if the aggregate processor
     * should not pass an event
     * @since 1.3
     */
    default AggregateActionOutput concludeGroup(final AggregateActionInput aggregateActionInput) {
        return new AggregateActionOutput(List.of());
    }

}
