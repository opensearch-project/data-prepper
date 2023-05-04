/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionInput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionOutput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionResponse;
import org.opensearch.dataprepper.plugins.processor.aggregate.GroupState;

import java.util.List;

/**
 * An AggregateAction that combines multiple Events into a single Event. This action will add the unique keys of each smaller Event to the overall groupState,
 * and will create a combined Event from the groupState on concludeGroup. If smaller Events have the same keys, then these keys will be overwritten with the keys of the
 * most recently handled Event.
 * @since 1.3
 */
@DataPrepperPlugin(name = "put_all", pluginType = AggregateAction.class)
public class PutAllAggregateAction implements AggregateAction {
    static final String EVENT_TYPE = "event";

    @Override
    public AggregateActionResponse handleEvent(final Event event, final AggregateActionInput aggregateActionInput) {
        final GroupState groupState = aggregateActionInput.getGroupState();
        groupState.putAll(event.toMap());
        return AggregateActionResponse.nullEventResponse();
    }

    @Override
    public AggregateActionOutput concludeGroup(final AggregateActionInput aggregateActionInput) {

        final Event event = JacksonEvent.builder()
                .withEventType(EVENT_TYPE)
                .withData(aggregateActionInput.getGroupState())
                .build();

        return new AggregateActionOutput(List.of(event));
    }
}
