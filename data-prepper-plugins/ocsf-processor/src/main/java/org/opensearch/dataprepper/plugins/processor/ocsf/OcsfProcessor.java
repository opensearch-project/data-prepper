/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.processor.ocsf;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collection;
import java.util.Map;

@DataPrepperPlugin(name = "ocsf_transform", pluginType = Processor.class, pluginConfigurationType = OcsfProcessorConfig.class)
public class OcsfProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(OcsfProcessor.class);

    private static final String METRIC_RECORDS_TRANSFORMED = "recordsTransformed";
    private static final String METRIC_RECORDS_FAILED = "recordsFailed";
    private static final String METRIC_PROCESSING_TIME = "processingTime";

    private final OcsfSchemaMapper schemaMapper;
    private final OcsfProcessorConfig config;
    private final Counter successCounter;
    private final Counter failureCounter;
    private final Timer processingTimer;

    @DataPrepperPluginConstructor
    public OcsfProcessor(final PluginMetrics pluginMetrics,
                         final OcsfProcessorConfig config) {
        super(pluginMetrics);
        this.config = config;
        this.schemaMapper = new OcsfSchemaMapper(config.getSchemaMapping());
        this.successCounter = pluginMetrics.counter(METRIC_RECORDS_TRANSFORMED);
        this.failureCounter = pluginMetrics.counter(METRIC_RECORDS_FAILED);
        this.processingTimer = pluginMetrics.timer(METRIC_PROCESSING_TIME);
    }

    @Override
    public Collection<Record<Event>> doExecute(Collection<Record<Event>> records) {
        Timer.Sample sample = Timer.start();
        try {
            for (Record<Event> record : records) {
                try {
                    Event event = record.getData();
                    Map<String, Object> sourceData = event.toMap();
                    Map<String, Object> ocsfData = schemaMapper.mapToOcsf(sourceData);
                    event.clear();
                    ocsfData.forEach(event::put);
                    successCounter.increment();
                } catch (Exception e) {
                    failureCounter.increment();
                    LOG.error("Failed to transform record to OCSF: {}", e.getMessage());
                    record.getData().getMetadata().addTags(config.getTagsOnFailure());
                }
            }
        } finally {
            sample.stop(processingTimer);
        }
        return records;
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