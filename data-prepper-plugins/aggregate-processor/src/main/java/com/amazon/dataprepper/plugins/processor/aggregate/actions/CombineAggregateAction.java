/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.aggregate.actions;

import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.JacksonEvent;
import com.amazon.dataprepper.plugins.processor.aggregate.AggregateAction;
import com.amazon.dataprepper.plugins.processor.aggregate.AggregateActionResponse;

import java.util.Map;
import java.util.Optional;

/**
 * An AggregateAction that combines multiple Events into a single Event. This action will add the unique keys of each smaller Event to the overall groupState,
 * and will create a combined Event from the groupState on concludeGroup. If smaller Events have the same keys, then these keys will be overwritten with the keys of the
 * most recently handled Event.
 * @since 1.3
 */
public class CombineAggregateAction implements AggregateAction {
    static final String EVENT_TYPE = "event";

    @Override
    public AggregateActionResponse handleEvent(final Event event, final Map<Object, Object> groupState) {
        groupState.putAll(event.toMap());
        return AggregateActionResponse.emptyEventResponse();
    }

    @Override
    public Optional<Event> concludeGroup(final Map<Object, Object> groupState) {
        final Event event = JacksonEvent.builder()
                .withEventType(EVENT_TYPE)
                .withData(groupState)
                .build();

        return Optional.of(event);
    }
}
