/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionInput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionOutput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionResponse;
import org.opensearch.dataprepper.plugins.processor.aggregate.GroupState;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * An AggregateAction that combines multiple Events into a single Event. This
 * action will add the unique keys of each smaller Event to the overall
 * groupState,
 * and will create a combined Event from the groupState on concludeGroup. If
 * smaller Events have the same keys, then the value for those keys would be appended in group state.
 * The append action is controlled by the keys_to_append field in config. If this field is not empty,
 * the append action will choose only the listed keys.
 *
 * @since 2.2
 */
@DataPrepperPlugin(name = "append", pluginType = AggregateAction.class, pluginConfigurationType = AppendAggregateActionConfig.class)
public class AppendAggregateAction implements AggregateAction {
    static final String EVENT_TYPE = "event";
    public final List<String> keysToAppend;

    @DataPrepperPluginConstructor
    public AppendAggregateAction(final AppendAggregateActionConfig appendAggregateActionConfig) {
        this.keysToAppend = appendAggregateActionConfig.getKeysToAppend();
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
            return AggregateActionResponse.nullEventResponse();
        }
        consumeEvent(groupState, event);
        return AggregateActionResponse.nullEventResponse();
    }

    private void consumeEvent(GroupState groupState, Event event) {
        // For each key in event if the key is defined in keysToAppend, then merge the values in groupState.
        event.toMap().forEach((key, value) -> {
            if (this.keysToAppend == null || keysToAppend.isEmpty() || this.keysToAppend.contains(key)) {
                Object valueFromGroupState = groupState.getOrDefault(key, value);
                if (valueFromGroupState instanceof List) {
                    if (value instanceof List) {
                        ((List) valueFromGroupState).addAll((List) value);
                    } else {
                        ((List) valueFromGroupState).add(value);
                    }
                } else {
                    if (!Objects.equals(value, valueFromGroupState)) {
                        groupState.put(key, Arrays.asList(valueFromGroupState, value));
                    }
                }
            }
        });
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
