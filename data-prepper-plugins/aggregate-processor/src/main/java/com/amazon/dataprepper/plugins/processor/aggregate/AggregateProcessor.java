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
    private final AggregateGroupManager aggregateGroupManager;
    private final AggregateActionSynchronizer aggregateActionSynchronizer;
    private final AggregateIdentificationKeysHasher aggregateIdentificationKeysHasher;

    @DataPrepperPluginConstructor
    public AggregateProcessor(final AggregateProcessorConfig aggregateProcessorConfig, final PluginMetrics pluginMetrics, final PluginFactory pluginFactory) {
        this(aggregateProcessorConfig, pluginMetrics, pluginFactory, new AggregateGroupManager(aggregateProcessorConfig.getWindowDuration()),
                new AggregateIdentificationKeysHasher(aggregateProcessorConfig.getIdentificationKeys()), new AggregateActionSynchronizer.AggregateActionSynchronizerProvider());
    }
    public AggregateProcessor(final AggregateProcessorConfig aggregateProcessorConfig, final PluginMetrics pluginMetrics, final PluginFactory pluginFactory, final AggregateGroupManager aggregateGroupManager,
                              final AggregateIdentificationKeysHasher aggregateIdentificationKeysHasher, final AggregateActionSynchronizer.AggregateActionSynchronizerProvider aggregateActionSynchronizerProvider) {
        super(pluginMetrics);
        this.aggregateProcessorConfig = aggregateProcessorConfig;
        this.aggregateGroupManager = aggregateGroupManager;
        this.aggregateIdentificationKeysHasher = aggregateIdentificationKeysHasher;
        AggregateAction aggregateAction = loadAggregateAction(pluginFactory);
        this.aggregateActionSynchronizer = aggregateActionSynchronizerProvider.provide(aggregateAction, aggregateGroupManager);
    }

    private AggregateAction loadAggregateAction(final PluginFactory pluginFactory) {
        final PluginModel actionConfiguration = aggregateProcessorConfig.getAggregateAction();
        final PluginSetting actionPluginSetting = new PluginSetting(actionConfiguration.getPluginName(), actionConfiguration.getPluginSettings());
        return pluginFactory.loadPlugin(AggregateAction.class, actionPluginSetting);
    }

    @Override
    public Collection<Record<Event>> doExecute(Collection<Record<Event>> records) {
        final List<Record<Event>> recordsOut = new LinkedList<>();

        final List<Map.Entry<AggregateIdentificationKeysHasher.IdentificationHash, AggregateGroup>> groupsToConclude = aggregateGroupManager.getGroupsToConclude();

        for (final Map.Entry<AggregateIdentificationKeysHasher.IdentificationHash, AggregateGroup> groupEntry : groupsToConclude) {
            final Optional<Event> concludeGroupEvent = aggregateActionSynchronizer.concludeGroup(groupEntry.getKey(), groupEntry.getValue());
            concludeGroupEvent.ifPresent(value -> recordsOut.add(new Record<>(value)));
        }

        for (final Record<Event> record : records) {
            final Event event = record.getData();
            final AggregateIdentificationKeysHasher.IdentificationHash identificationKeysHash = aggregateIdentificationKeysHasher.createIdentificationKeyHashFromEvent(event);
            final AggregateGroup aggregateGroupForEvent = aggregateGroupManager.getAggregateGroup(identificationKeysHash);

            final AggregateActionResponse handleEventResponse = aggregateActionSynchronizer.handleEventForGroup(event, identificationKeysHash, aggregateGroupForEvent);

            final Event aggregateActionResponseEvent = handleEventResponse.getEvent();

            if (aggregateActionResponseEvent != null) {
                recordsOut.add(new Record<>(aggregateActionResponseEvent, record.getMetadata()));
            }
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
