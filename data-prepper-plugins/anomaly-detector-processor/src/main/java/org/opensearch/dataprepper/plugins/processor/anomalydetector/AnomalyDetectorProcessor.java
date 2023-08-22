/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.anomalydetector;


import io.micrometer.core.instrument.Counter;
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
import org.opensearch.dataprepper.plugins.hasher.IdentificationKeysHasher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

@DataPrepperPlugin(name = "anomaly_detector", pluginType = Processor.class, pluginConfigurationType = AnomalyDetectorProcessorConfig.class)
public class AnomalyDetectorProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    public static final String DEVIATION_KEY = "deviation_from_expected";
    public static final String GRADE_KEY = "grade";
    static final String NUMBER_RCF_INSTANCES = "RCFInstances";
    static final String CARDINALITY_OVERFLOW = "cardinalityOverflow";

    private final Boolean verbose;
    private final int cardinalityLimit;
    private final IdentificationKeysHasher identificationKeysHasher;
    private final List<String> keys;
    private final PluginFactory pluginFactory;
    private final HashMap<Integer, AnomalyDetectorMode> forestMap;
    private final AtomicInteger cardinality;
    private final AnomalyDetectorProcessorConfig anomalyDetectorProcessorConfig;
    private static final Logger LOG = LoggerFactory.getLogger(AnomalyDetectorProcessor.class);
    private final Counter cardinalityOverflowCounter;
    Instant nextWarnTime = Instant.MIN;
    @DataPrepperPluginConstructor
    public AnomalyDetectorProcessor(final AnomalyDetectorProcessorConfig anomalyDetectorProcessorConfig, final PluginMetrics pluginMetrics, final PluginFactory pluginFactory) {
        super(pluginMetrics);
        this.identificationKeysHasher = new IdentificationKeysHasher(anomalyDetectorProcessorConfig.getIdentificationKeys());
        this.anomalyDetectorProcessorConfig = anomalyDetectorProcessorConfig;
        this.pluginFactory = pluginFactory;
        this.keys = anomalyDetectorProcessorConfig.getKeys();
        this.verbose = anomalyDetectorProcessorConfig.getVerbose();
        this.cardinality = pluginMetrics.gauge(NUMBER_RCF_INSTANCES, new AtomicInteger());
        this.cardinalityLimit = anomalyDetectorProcessorConfig.getCardinalityLimit();
        this.cardinalityOverflowCounter = pluginMetrics.counter(CARDINALITY_OVERFLOW);
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
            final IdentificationKeysHasher.IdentificationKeysMap identificationKeysMap = identificationKeysHasher.createIdentificationKeysMapFromEvent(event);
            AnomalyDetectorMode forest = forestMap.get(identificationKeysMap.hashCode());

            if (Objects.nonNull(forest)) {
                recordsOut.addAll(forest.handleEvents(List.of(record)));
            } else if (forestMap.size() < cardinalityLimit) {
                forest = loadAnomalyDetectorMode(pluginFactory);
                forest.initialize(keys, verbose);
                forestMap.put(identificationKeysMap.hashCode(), forest);
                recordsOut.addAll(forest.handleEvents(List.of(record)));
            } else {
                if (Instant.now().isAfter(nextWarnTime)) {
                    LOG.warn("Cardinality limit reached, see cardinalityOverflow metric for count of skipped records");
                    nextWarnTime = Instant.now().plus(5, ChronoUnit.MINUTES);
                }
                cardinalityOverflowCounter.increment();
            }
        }
        cardinality.set(forestMap.size());
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
