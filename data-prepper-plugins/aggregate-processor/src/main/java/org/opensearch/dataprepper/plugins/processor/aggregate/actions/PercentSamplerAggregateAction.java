/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionInput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionResponse;
import org.opensearch.dataprepper.plugins.processor.aggregate.GroupState;

/**
 * An AggregateAction that combines multiple Events into a single Event. This action 
 * 
 * @since 2.1
 */
@DataPrepperPlugin(name = "percent_sampler", pluginType = AggregateAction.class, pluginConfigurationType = PercentSamplerAggregateActionConfig.class)
public class PercentSamplerAggregateAction implements AggregateAction {
    static final String TOTAL_EVENTS_KEY = "total_events";
    static final String TOTAL_ALLOWED_EVENTS_KEY = "total_allowed_events";
    private final double percent;

    @DataPrepperPluginConstructor
    public PercentSamplerAggregateAction(final PercentSamplerAggregateActionConfig percentSamplerAggregateActionConfig) {
        percent = percentSamplerAggregateActionConfig.getPercent();
    }

    @Override
    public AggregateActionResponse handleEvent(final Event event, final AggregateActionInput aggregateActionInput) {
        final GroupState groupState = aggregateActionInput.getGroupState();
        Long totalEvents = 0L;
        Long totalAllowedEvents = 0L;
        if (groupState.get(TOTAL_EVENTS_KEY) == null) {
            groupState.put(TOTAL_ALLOWED_EVENTS_KEY, 0L);
        } else {
            totalEvents = (Long)groupState.get(TOTAL_EVENTS_KEY);
            totalAllowedEvents = (Long)groupState.get(TOTAL_ALLOWED_EVENTS_KEY);
        }
        groupState.put(TOTAL_EVENTS_KEY, totalEvents+1);
        if ((((double)(totalAllowedEvents+1))/(double)(totalEvents+1)) <= percent/100.0) {
            groupState.put(TOTAL_ALLOWED_EVENTS_KEY, totalAllowedEvents+1);
            return new AggregateActionResponse(event);
        }
        return AggregateActionResponse.nullEventResponse();
    }

}
