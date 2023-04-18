/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.peerforwarder.RequiresPeerForwarding;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import io.micrometer.core.instrument.Counter;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.time.Instant;

@DataPrepperPlugin(name = "aggregate", pluginType = Processor.class, pluginConfigurationType = AggregateProcessorConfig.class)
public class AggregateProcessor extends AbstractProcessor<Record<Event>, Record<Event>> implements RequiresPeerForwarding {
    static final String ACTION_HANDLE_EVENTS_OUT = "actionHandleEventsOut";
    static final String ACTION_HANDLE_EVENTS_DROPPED = "actionHandleEventsDropped";
    static final String ACTION_CONCLUDE_GROUP_EVENTS_OUT = "actionConcludeGroupEventsOut";
    static final String ACTION_CONCLUDE_GROUP_EVENTS_DROPPED = "actionConcludeGroupEventsDropped";
    static final String CURRENT_AGGREGATE_GROUPS = "currentAggregateGroups";

    private final Counter actionHandleEventsOutCounter;
    private final Counter actionHandleEventsDroppedCounter;
    private final Counter actionConcludeGroupEventsDroppedCounter;
    private final Counter actionConcludeGroupEventsOutCounter;

    private final AggregateProcessorConfig aggregateProcessorConfig;
    private final AggregateGroupManager aggregateGroupManager;
    private final AggregateActionSynchronizer aggregateActionSynchronizer;
    private final AggregateIdentificationKeysHasher aggregateIdentificationKeysHasher;
    private final AggregateAction aggregateAction;

    private boolean forceConclude = false;
    private final String whenCondition;
    private final ExpressionEvaluator<Boolean> expressionEvaluator;

    @DataPrepperPluginConstructor
    public AggregateProcessor(final AggregateProcessorConfig aggregateProcessorConfig, final PluginMetrics pluginMetrics, final PluginFactory pluginFactory, final ExpressionEvaluator<Boolean> expressionEvaluator) {
        this(aggregateProcessorConfig, pluginMetrics, pluginFactory, new AggregateGroupManager(aggregateProcessorConfig.getGroupDuration()),
                new AggregateIdentificationKeysHasher(aggregateProcessorConfig.getIdentificationKeys()), new AggregateActionSynchronizer.AggregateActionSynchronizerProvider(), expressionEvaluator);
    }
    public AggregateProcessor(final AggregateProcessorConfig aggregateProcessorConfig, final PluginMetrics pluginMetrics, final PluginFactory pluginFactory, final AggregateGroupManager aggregateGroupManager,
                              final AggregateIdentificationKeysHasher aggregateIdentificationKeysHasher, final AggregateActionSynchronizer.AggregateActionSynchronizerProvider aggregateActionSynchronizerProvider, final ExpressionEvaluator<Boolean> expressionEvaluator) {
        super(pluginMetrics);
        this.aggregateProcessorConfig = aggregateProcessorConfig;
        this.aggregateGroupManager = aggregateGroupManager;
        this.expressionEvaluator = expressionEvaluator;
        this.aggregateIdentificationKeysHasher = aggregateIdentificationKeysHasher;
        this.aggregateAction = loadAggregateAction(pluginFactory);
        this.aggregateActionSynchronizer = aggregateActionSynchronizerProvider.provide(aggregateAction, aggregateGroupManager, pluginMetrics);

        this.actionConcludeGroupEventsOutCounter = pluginMetrics.counter(ACTION_CONCLUDE_GROUP_EVENTS_OUT);
        this.actionConcludeGroupEventsDroppedCounter = pluginMetrics.counter(ACTION_CONCLUDE_GROUP_EVENTS_DROPPED);
        this.actionHandleEventsOutCounter = pluginMetrics.counter(ACTION_HANDLE_EVENTS_OUT);
        this.actionHandleEventsDroppedCounter = pluginMetrics.counter(ACTION_HANDLE_EVENTS_DROPPED);
        this.whenCondition = aggregateProcessorConfig.getWhenCondition();

        pluginMetrics.gauge(CURRENT_AGGREGATE_GROUPS, aggregateGroupManager, AggregateGroupManager::getAllGroupsSize);
    }

    private AggregateAction loadAggregateAction(final PluginFactory pluginFactory) {
        final PluginModel actionConfiguration = aggregateProcessorConfig.getAggregateAction();
        final PluginSetting actionPluginSetting = new PluginSetting(actionConfiguration.getPluginName(), actionConfiguration.getPluginSettings());
        return pluginFactory.loadPlugin(AggregateAction.class, actionPluginSetting);
    }

    @Override
    public Collection<Record<Event>> doExecute(Collection<Record<Event>> records) {
        final List<Record<Event>> recordsOut = new LinkedList<>();

        final List<Map.Entry<AggregateIdentificationKeysHasher.IdentificationKeysMap, AggregateGroup>> groupsToConclude = aggregateGroupManager.getGroupsToConclude(forceConclude);
        for (final Map.Entry<AggregateIdentificationKeysHasher.IdentificationKeysMap, AggregateGroup> groupEntry : groupsToConclude) {
            final AggregateActionOutput actionOutput = aggregateActionSynchronizer.concludeGroup(groupEntry.getKey(), groupEntry.getValue(), forceConclude);

            final List<Event> concludeGroupEvents = actionOutput != null ? actionOutput.getEvents() : null;
            if (concludeGroupEvents != null && !concludeGroupEvents.isEmpty()) {
                concludeGroupEvents.stream().forEach((event) -> {
                    recordsOut.add(new Record(event));
                    actionConcludeGroupEventsOutCounter.increment();
                });
            } else {
                actionConcludeGroupEventsDroppedCounter.increment();
            }
        }

        int handleEventsOut = 0;
        int handleEventsDropped = 0;
        for (final Record<Event> record : records) {
            final Event event = record.getData();
            if (whenCondition != null && !expressionEvaluator.evaluate(whenCondition, event)) {
                handleEventsDropped++;
                continue;
            }
            final AggregateIdentificationKeysHasher.IdentificationKeysMap identificationKeysMap = aggregateIdentificationKeysHasher.createIdentificationKeysMapFromEvent(event);
            final AggregateGroup aggregateGroupForEvent = aggregateGroupManager.getAggregateGroup(identificationKeysMap);

            final AggregateActionResponse handleEventResponse = aggregateActionSynchronizer.handleEventForGroup(event, identificationKeysMap, aggregateGroupForEvent);

            final Event aggregateActionResponseEvent = handleEventResponse.getEvent();

            if (aggregateActionResponseEvent != null) {
                recordsOut.add(new Record<>(aggregateActionResponseEvent, record.getMetadata()));
                handleEventsOut++;
            } else {
                handleEventsDropped++;
            }
        }

        actionHandleEventsOutCounter.increment(handleEventsOut);
        actionHandleEventsDroppedCounter.increment(handleEventsDropped);
        return recordsOut;
    }

    public static long getTimeNanos(final Instant time) {
        final long NANO_MULTIPLIER = 1_000 * 1_000 * 1_000;
        long currentTimeNanos = time.getEpochSecond() * NANO_MULTIPLIER + time.getNano();
        return currentTimeNanos;
    }


    @Override
    public void prepareForShutdown() {
        forceConclude = true;
    }

    @Override
    public boolean isReadyForShutdown() {
        return aggregateGroupManager.getAllGroupsSize() == 0;
    }

    @Override
    public void shutdown() {

    }

    @Override
    public Collection<String> getIdentificationKeys() {
        return aggregateProcessorConfig.getIdentificationKeys();
    }
}
