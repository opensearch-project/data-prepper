/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.anomalydetector;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateIdentificationKeysHasher;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

@DataPrepperPlugin(name = "anomaly_detector", pluginType = Processor.class, pluginConfigurationType = AnomalyDetectorProcessorConfig.class)
public class AnomalyDetectorProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    public static final String DEVIATION_KEY = "deviation_from_expected";
    public static final String GRADE_KEY = "grade";

    private final AggregateIdentificationKeysHasher aggregateIdentificationKeysHasher;
    private final List<String> keys;
    private final PluginFactory pluginFactory;
    private final HashMap<Integer, AnomalyDetectorMode> forestMap;
    private final AnomalyDetectorProcessorConfig anomalyDetectorProcessorConfig;

    @DataPrepperPluginConstructor
    public AnomalyDetectorProcessor(final AnomalyDetectorProcessorConfig anomalyDetectorProcessorConfig, final PluginMetrics pluginMetrics, final PluginFactory pluginFactory) {
        super(pluginMetrics);
        this.aggregateIdentificationKeysHasher = new AggregateIdentificationKeysHasher(anomalyDetectorProcessorConfig.getIdentificationKeys());
        this.anomalyDetectorProcessorConfig = anomalyDetectorProcessorConfig;
        this.pluginFactory = pluginFactory;
        keys = anomalyDetectorProcessorConfig.getKeys();
        forestMap = new HashMap<>();
    }

    private AnomalyDetectorMode loadAnomalyDetectorMode(final PluginFactory pluginFactory) {
        final PluginModel modeConfiguration = anomalyDetectorProcessorConfig.getDetectorMode();
        final PluginSetting modePluginSetting = new PluginSetting(modeConfiguration.getPluginName(), modeConfiguration.getPluginSettings());
        return pluginFactory.loadPlugin(AnomalyDetectorMode.class, modePluginSetting);
    }

    @Override
    public Collection<Record<Event>> doExecute(Collection<Record<Event>> records) {
        final List<Record<Event>> recordsOut = new LinkedList<>();

        for (final Record<Event> record : records) {
            final Event event = record.getData();
            // If user has not configured IdentificationKeys, the empty set will always hash to "31",
            // so the same forest will be used, and we don't need to write a special case.
            final AggregateIdentificationKeysHasher.IdentificationKeysMap identificationKeysMap = aggregateIdentificationKeysHasher.createIdentificationKeysMapFromEvent(event);
            if (!forestMap.containsKey(identificationKeysMap.hashCode())) {
                final AnomalyDetectorMode hashMode = loadAnomalyDetectorMode(pluginFactory);
                hashMode.initialize(keys);
                forestMap.put(identificationKeysMap.hashCode(), hashMode);
            }
            recordsOut.addAll(forestMap.get(identificationKeysMap.hashCode()).handleEvents(List.of(record)));
        }
        return recordsOut;
    }


    @Override
    public void prepareForShutdown() {

    }

    @Override
    public boolean isReadyForShutdown() {
        return true;
    }

    @Override
    public void shutdown() {

    }
}
