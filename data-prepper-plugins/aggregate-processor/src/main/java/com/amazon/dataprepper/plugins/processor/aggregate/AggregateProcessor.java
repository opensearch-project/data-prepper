/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.aggregate;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.annotations.DataPrepperPluginConstructor;
import com.amazon.dataprepper.model.configuration.PluginModel;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.plugin.PluginFactory;
import com.amazon.dataprepper.model.processor.AbstractProcessor;
import com.amazon.dataprepper.model.processor.Processor;
import com.amazon.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@DataPrepperPlugin(name = "aggregate", pluginType = Processor.class, pluginConfigurationType = AggregateProcessorConfig.class)
public class AggregateProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(AggregateProcessor.class);

    private final AggregateProcessorConfig aggregateProcessorConfig;
    private final GroupStateManager groupStateManager;
    private final AggregateAction aggregateAction;
    private final AggregateIdentificationKeysHasher aggregateIdentificationKeysHasher;

    @DataPrepperPluginConstructor
    public AggregateProcessor(final AggregateProcessorConfig aggregateProcessorConfig, final PluginMetrics pluginMetrics, final PluginFactory pluginFactory) {
        this(aggregateProcessorConfig, pluginMetrics, pluginFactory, new GroupStateManager(), new AggregateIdentificationKeysHasher());
    }
    public AggregateProcessor(final AggregateProcessorConfig aggregateProcessorConfig, final PluginMetrics pluginMetrics, final PluginFactory pluginFactory, final GroupStateManager groupStateManager, final AggregateIdentificationKeysHasher aggregateIdentificationKeysHasher) {
        super(pluginMetrics);
        this.aggregateProcessorConfig = aggregateProcessorConfig;
        this.groupStateManager = groupStateManager;
        this.aggregateIdentificationKeysHasher = aggregateIdentificationKeysHasher;
        this.aggregateAction = loadAggregateAction(pluginFactory);
    }

    private AggregateAction loadAggregateAction(final PluginFactory pluginFactory) {
        final PluginModel actionConfiguration = aggregateProcessorConfig.getAggregateAction();
        final PluginSetting actionPluginSetting = new PluginSetting(actionConfiguration.getPluginName(), actionConfiguration.getPluginSettings());
        return pluginFactory.loadPlugin(AggregateAction.class, actionPluginSetting);
    }

    @Override
    public Collection<Record<Event>> doExecute(Collection<Record<Event>> records) {
        final List<Record<Event>> recordsOut = new LinkedList<>();

        for (final Record<Event> record : records) {
            final Event event = record.getData();
            final Map<Object, Object> identificationKeysHash = aggregateIdentificationKeysHasher.createIdentificationKeyHashFromEvent(event, aggregateProcessorConfig.getIdentificationKeys());
            final Map<Object, Object> groupStateForEvent = groupStateManager.getGroupState(identificationKeysHash);

            final AggregateActionResponse handleEventResponse = aggregateAction.handleEvent(event, groupStateForEvent);

            final Optional<Event> aggregateActionResponseEvent = Optional.ofNullable(handleEventResponse.getEvent());
            aggregateActionResponseEvent.ifPresent(value -> recordsOut.add(new Record<>(value, record.getMetadata())));

        }
        return recordsOut;
    }

    @Override
    public void prepareForShutdown() {

    }

    @Override
    public boolean isReadyForShutdown() {
        return false;
    }

    @Override
    public void shutdown() {

    }
}
