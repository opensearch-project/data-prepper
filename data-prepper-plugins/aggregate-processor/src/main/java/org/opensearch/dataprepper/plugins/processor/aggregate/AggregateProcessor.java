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
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.plugins.hasher.IdentificationKeysHasher;

import java.math.BigDecimal;
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
    private final IdentificationKeysHasher identificationKeysHasher;
    private final AggregateAction aggregateAction;

    private boolean forceConclude = false;
    private boolean localMode = false;
    private final String whenCondition;
    private final ExpressionEvaluator expressionEvaluator;
    private final boolean outputUnaggregatedEvents;
    private final String aggregatedEventsTag;

    @DataPrepperPluginConstructor
    public AggregateProcessor(final AggregateProcessorConfig aggregateProcessorConfig, final PluginMetrics pluginMetrics, final PluginFactory pluginFactory, final ExpressionEvaluator expressionEvaluator) {
        this(aggregateProcessorConfig, pluginMetrics, pluginFactory, new AggregateGroupManager(aggregateProcessorConfig.getGroupDuration()),
                new IdentificationKeysHasher(aggregateProcessorConfig.getIdentificationKeys()), new AggregateActionSynchronizer.AggregateActionSynchronizerProvider(), expressionEvaluator);
    }
    public AggregateProcessor(final AggregateProcessorConfig aggregateProcessorConfig, final PluginMetrics pluginMetrics, final PluginFactory pluginFactory, final AggregateGroupManager aggregateGroupManager,
                              final IdentificationKeysHasher identificationKeysHasher, final AggregateActionSynchronizer.AggregateActionSynchronizerProvider aggregateActionSynchronizerProvider, final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.aggregateProcessorConfig = aggregateProcessorConfig;
        this.aggregatedEventsTag = aggregateProcessorConfig.getAggregatedEventsTag();
        this.aggregateGroupManager = aggregateGroupManager;
        this.outputUnaggregatedEvents = aggregateProcessorConfig.getOutputUnaggregatedEvents();
        this.expressionEvaluator = expressionEvaluator;
        this.identificationKeysHasher = identificationKeysHasher;
        this.aggregateAction = loadAggregateAction(pluginFactory);
        this.aggregateActionSynchronizer = aggregateActionSynchronizerProvider.provide(aggregateAction, aggregateGroupManager, pluginMetrics);

        this.actionConcludeGroupEventsOutCounter = pluginMetrics.counter(ACTION_CONCLUDE_GROUP_EVENTS_OUT);
        this.actionConcludeGroupEventsDroppedCounter = pluginMetrics.counter(ACTION_CONCLUDE_GROUP_EVENTS_DROPPED);
        this.actionHandleEventsOutCounter = pluginMetrics.counter(ACTION_HANDLE_EVENTS_OUT);
        this.actionHandleEventsDroppedCounter = pluginMetrics.counter(ACTION_HANDLE_EVENTS_DROPPED);
        this.whenCondition = aggregateProcessorConfig.getWhenCondition();
        this.localMode = aggregateProcessorConfig.getLocalMode();

        pluginMetrics.gauge(CURRENT_AGGREGATE_GROUPS, aggregateGroupManager, AggregateGroupManager::getAllGroupsSize);

        if (aggregateProcessorConfig.getWhenCondition() != null && (!expressionEvaluator.isValidExpressionStatement(aggregateProcessorConfig.getWhenCondition()))) {
            throw new InvalidPluginConfigurationException("aggregate_when {} is not a valid expression statement. See https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/ for valid expression syntax");
        }
    }

    private AggregateAction loadAggregateAction(final PluginFactory pluginFactory) {
        final PluginModel actionConfiguration = aggregateProcessorConfig.getAggregateAction();
        final PluginSetting actionPluginSetting = new PluginSetting(actionConfiguration.getPluginName(), actionConfiguration.getPluginSettings());
        return pluginFactory.loadPlugin(AggregateAction.class, actionPluginSetting);
    }

    @Override
    public Collection<Record<Event>> doExecute(Collection<Record<Event>> records) {
        final List<Record<Event>> recordsOut = new LinkedList<>();

        final List<Map.Entry<IdentificationKeysHasher.IdentificationKeysMap, AggregateGroup>> groupsToConclude = aggregateGroupManager.getGroupsToConclude(forceConclude);
        for (final Map.Entry<IdentificationKeysHasher.IdentificationKeysMap, AggregateGroup> groupEntry : groupsToConclude) {
            final AggregateActionOutput actionOutput = aggregateActionSynchronizer.concludeGroup(groupEntry.getKey(), groupEntry.getValue(), forceConclude);

            final List<Event> concludeGroupEvents = actionOutput != null ? actionOutput.getEvents() : null;
            if (!concludeGroupEvents.isEmpty()) {
                concludeGroupEvents.stream().forEach((event) -> {
                    if (aggregatedEventsTag != null) {
                        event.getMetadata().addTags(List.of(aggregatedEventsTag));
                    }
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
            if (whenCondition != null && !expressionEvaluator.evaluateConditional(whenCondition, event)) {
                handleEventsDropped++;
                continue;
            }
            final IdentificationKeysHasher.IdentificationKeysMap identificationKeysMap = identificationKeysHasher.createIdentificationKeysMapFromEvent(event);
            final AggregateGroup aggregateGroupForEvent = aggregateGroupManager.getAggregateGroup(identificationKeysMap);

            final AggregateActionResponse handleEventResponse = aggregateActionSynchronizer.handleEventForGroup(event, identificationKeysMap, aggregateGroupForEvent);

            final Event aggregateActionResponseEvent = handleEventResponse.getEvent();

            if (aggregateActionResponseEvent != null) {
                if (aggregatedEventsTag != null) {
                    aggregateActionResponseEvent.getMetadata().addTags(List.of(aggregatedEventsTag));
                }
                recordsOut.add(new Record<>(aggregateActionResponseEvent, record.getMetadata()));
                handleEventsOut++;
            } else {
                handleEventsDropped++;
            }
            if (outputUnaggregatedEvents) {
                recordsOut.add(record);
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

    public static Instant convertObjectToInstant(Object timeObject) {
        if (timeObject instanceof Instant) {
            return (Instant)timeObject;
        } else if (timeObject instanceof String) {
            return Instant.parse((String)timeObject);
        } else if (timeObject instanceof Integer || timeObject instanceof Long) {
            long value = ((Number)timeObject).longValue();
            return (value > 1E10) ? Instant.ofEpochMilli(value) : Instant.ofEpochSecond(value);
        } else if (timeObject instanceof Double || timeObject instanceof Float || timeObject instanceof BigDecimal) {
            double value = ((Number)timeObject).doubleValue();
            long seconds = (long) value;
            long nanos = (long) ((value - seconds) * 1_000_000_000);
            return Instant.ofEpochSecond(seconds, nanos);
        } else {
            throw new RuntimeException("Invalid format for time "+timeObject);
        }
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
    public boolean isForLocalProcessingOnly(Event event) {
        // no need to check for when condition here because it is
        // done in doExecute(). isApplicableEventForPeerForwarding()
        // checks for when condition because it is an optimization to
        // not send events not matching the condition to remote peer
        // only to be skipped later.
        return localMode;
    }

    @Override
    public boolean isApplicableEventForPeerForwarding(Event event) {
        if (localMode) {
            return false;
        }
        if (whenCondition == null) {
            return true;
        }
        return expressionEvaluator.evaluateConditional(whenCondition, event);
    }

    @Override
    public Collection<String> getIdentificationKeys() {
        return aggregateProcessorConfig.getIdentificationKeys();
    }
}
