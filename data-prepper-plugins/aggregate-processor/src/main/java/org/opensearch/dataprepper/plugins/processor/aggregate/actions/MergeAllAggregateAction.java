/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionInput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionResponse;
import org.opensearch.dataprepper.plugins.processor.aggregate.GroupState;

/**
 * An AggregateAction that combines multiple Events into a single Event. This
 * action will add the unique keys of each smaller Event to the overall
 * groupState,
 * and will create a combined Event from the groupState on concludeGroup. If
 * smaller Events have the same keys, then these keys will be overwritten with
 * the keys of the
 * most recently handled Event.
 * 
 * @since 1.3
 */
@DataPrepperPlugin(name = "merge_all", pluginType = AggregateAction.class, pluginConfigurationType = MergeAllAggregateActionConfig.class)
public class MergeAllAggregateAction implements AggregateAction {
    static final String EVENT_TYPE = "event";
    public final Map<String, String> dataTypeMap;

    @DataPrepperPluginConstructor
    public MergeAllAggregateAction(final MergeAllAggregateActionConfig mergeAllAggregateActionConfig) {
        this.dataTypeMap = mergeAllAggregateActionConfig.getDataTypes();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.opensearch.dataprepper.plugins.processor.aggregate.AggregateAction#
     * handleEvent(org.opensearch.dataprepper.model.event.Event,
     * org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionInput)
     */
    @Override
    public AggregateActionResponse handleEvent(final Event event, final AggregateActionInput aggregateActionInput) {
        final GroupState groupState = aggregateActionInput.getGroupState();
        if (groupState.size() == 0) {
            groupState.putAll(event.toMap());
        }
        Set<String> eventKeys = event.toMap().keySet();
        for (String key : eventKeys) {
            if (!this.dataTypeMap.containsKey(key)) {
                continue;
            }
            Object value = null;
            String dataType = this.dataTypeMap.get(key);
            switch (dataType) {
                case "integer":
                    value = event.get(key, Integer.class);
                    break;

                case "string":
                    value = event.get(key, String.class);
                    break;
            }

            Object valueFromGroupState = groupState.getOrDefault(key, value);
            List<Object> listValues = null;
            if (valueFromGroupState instanceof List) {
                listValues = (List) valueFromGroupState;
                listValues.add(value);
            } else {
                if (!value.equals(valueFromGroupState)) {
                    listValues = new ArrayList<>();
                    listValues.add(valueFromGroupState);
                    listValues.add(value);
                    groupState.put(key, listValues);
                }
            }
        }
        return AggregateActionResponse.nullEventResponse();
    }

    @Override
    public Optional<Event> concludeGroup(final AggregateActionInput aggregateActionInput) {

        final Event event = JacksonEvent.builder()
                .withEventType(EVENT_TYPE)
                .withData(aggregateActionInput.getGroupState())
                .build();
        return Optional.of(event);
    }
}
