/*
 * Copyright OpenSearch Contributors
 * SPDX-Limense-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate.actions;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateAction;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionInput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionOutput;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateActionResponse;
import org.opensearch.dataprepper.plugins.processor.aggregate.GroupState;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.time.Duration;
import java.time.Instant;

/**
 * An AggregateAction that combines multiple Events into a single Event. This action 
 * 
 * @since 2.3
 */
@DataPrepperPlugin(name = "tail_sampler", pluginType = AggregateAction.class, pluginConfigurationType = TailSamplerAggregateActionConfig.class)
public class TailSamplerAggregateAction implements AggregateAction {
    static final String LAST_RECEIVED_TIME_KEY = "last_received_time";
    static final String SHOULD_CONCLUDE_CHECK_SET_KEY = "should_conclude_check_set";
    static final String EVENTS_KEY = "events";
    static final String ERROR_STATUS_KEY = "error_status";
    private final int percent;
    private final Duration waitPeriod;
    private final ExpressionEvaluator expressionEvaluator;
    private final String condition;
    private boolean shouldCarryGroupState;
    private Random random;

    @DataPrepperPluginConstructor
    public TailSamplerAggregateAction(final TailSamplerAggregateActionConfig tailSamplerAggregateActionConfig, final ExpressionEvaluator expressionEvaluator) {
        percent = tailSamplerAggregateActionConfig.getPercent();
        waitPeriod = tailSamplerAggregateActionConfig.getWaitPeriod();
        condition = tailSamplerAggregateActionConfig.getCondition();
        this.random = new Random();
        this.expressionEvaluator = expressionEvaluator;
        shouldCarryGroupState = true;
    }

    @Override
    public AggregateActionResponse handleEvent(final Event event, final AggregateActionInput aggregateActionInput) {
        final GroupState groupState = aggregateActionInput.getGroupState();
        if (!((boolean)groupState.getOrDefault(SHOULD_CONCLUDE_CHECK_SET_KEY, false))) {
            aggregateActionInput.setCustomShouldConclude((duration) -> {
                Duration timeDiff = Duration.between((Instant)groupState.getOrDefault(LAST_RECEIVED_TIME_KEY, Instant.now()), Instant.now());

                return timeDiff.toSeconds() >= waitPeriod.toSeconds();
            });
            groupState.put(SHOULD_CONCLUDE_CHECK_SET_KEY, true);
        }
        List<Event> events = (List)groupState.getOrDefault(EVENTS_KEY, new ArrayList<>());
        events.add(event);
        groupState.put(EVENTS_KEY, events);
        if (condition != null && !condition.isEmpty() && expressionEvaluator.evaluateConditional(condition, event)) {
            groupState.put(ERROR_STATUS_KEY, true);
        }
        groupState.put(LAST_RECEIVED_TIME_KEY, Instant.now());
        return AggregateActionResponse.nullEventResponse();
    }

    @Override
    public boolean holdsEvents() {
        return true;
    }

    @Override
    public AggregateActionOutput concludeGroup(final AggregateActionInput aggregateActionInput) {
        GroupState groupState = aggregateActionInput.getGroupState();
        int randomInt = random.nextInt(100);
        aggregateActionInput.getEventHandle().release(true);
        if (((groupState.containsKey(ERROR_STATUS_KEY) && (Boolean)groupState.get(ERROR_STATUS_KEY) == true)) || (randomInt < percent)) {
            return new AggregateActionOutput((List)groupState.getOrDefault(EVENTS_KEY, List.of()));
        }
        List<Event> events = (List)groupState.getOrDefault(EVENTS_KEY, List.of());
        for (final Event event : events) {
            EventHandle eventHandle = event.getEventHandle();
            if (eventHandle != null) {
                eventHandle.release(true);
            }
        }
        return new AggregateActionOutput(Collections.emptyList());
    }

}
