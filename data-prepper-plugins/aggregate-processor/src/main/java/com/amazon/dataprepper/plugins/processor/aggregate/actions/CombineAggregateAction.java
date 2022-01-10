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

public class CombineAggregateAction implements AggregateAction {
    static final String COMBINED_EVENT_TYPE = "combined_event";

    @Override
    public AggregateActionResponse handleEvent(Event event, Map<Object, Object> groupState) {
        if (groupState != null)
            groupState.putAll(event.toMap());

        return new AggregateActionResponse(Optional.empty(), false);
    }

    @Override
    public Optional<Event> concludeGroup(Map<Object, Object> groupState) {
        final Event event = JacksonEvent.builder()
                .withEventType(COMBINED_EVENT_TYPE)
                .withData(groupState)
                .build();

        return Optional.of(event);
    }
}
