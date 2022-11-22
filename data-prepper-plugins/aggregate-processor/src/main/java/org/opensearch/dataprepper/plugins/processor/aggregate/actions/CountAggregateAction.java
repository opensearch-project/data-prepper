/*
 * C
 * SPDX-Limense-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionInput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionResponse;
import org.opensearch.dataprepper.plugins.processor.aggregate.GroupState;

import java.util.Optional;

/**
 * An AggregateAction that combines multiple Events into a single Event. This action will add the unique keys of each smaller Event to the overall groupState,
 * and will create a combined Event from the groupState on concludeGroup. If smaller Events have the same keys, then these keys will be overwritten with the keys of the
 * most recently handled Event.
 * @since 1.3
 */
@DataPrepperPlugin(name = "count", pluginType = AggregateAction.class)
public class CountAggregateAction implements AggregateAction {
    static final String EVENT_TYPE = "event";
    // COUNTKEY string is chosen to be unique and NOT to conflict with user provided keys
    // This COUNTKEY is used by OTEL/Prometheus format converter processor to generate a SUMMARY metric
    public static final String COUNTKEY = "aggr._count";

    @Override
    public AggregateActionResponse handleEvent(final Event event, final AggregateActionInput aggregateActionInput) {
        final GroupState groupState = aggregateActionInput.getGroupState();
        if (groupState.get(COUNTKEY) == null) {
            groupState.put(COUNTKEY, 1);
        } else {
            Integer v = (Integer)groupState.get(COUNTKEY) + 1;
            groupState.put(COUNTKEY, v);
        } 
        return AggregateActionResponse.nullEventResponse();
    }

    @Override
    public Optional<Event> concludeGroup(final AggregateActionInput aggregateActionInput) {

        GroupState groupState = aggregateActionInput.getGroupState();
        final Event event = JacksonEvent.builder()
                .withEventType(EVENT_TYPE)
                .withData(groupState)
                .build();
        
        return Optional.of(event);
    }

    @Override
    public boolean useOnlyIdentificationKeys() {
        return true;
    }
    
}
