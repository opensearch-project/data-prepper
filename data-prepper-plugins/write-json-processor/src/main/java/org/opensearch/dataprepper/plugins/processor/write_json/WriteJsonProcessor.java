/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.write_json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.plugin.PluginFactory;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

@DataPrepperPlugin(name = "write_json", pluginType = Processor.class, pluginConfigurationType = WriteJsonProcessorConfig.class)
public class WriteJsonProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(WriteJsonProcessor.class);
    private final String source;
    private String target;
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @DataPrepperPluginConstructor
    public WriteJsonProcessor(final WriteJsonProcessorConfig writeJsonProcessorConfig, final PluginMetrics pluginMetrics, final PluginFactory pluginFactory) {
        super(pluginMetrics);
        source = writeJsonProcessorConfig.getSource();
        target = writeJsonProcessorConfig.getTarget();
        if (target == null) {
            target = source;
        }
    }

    @Override
    public Collection<Record<Event>> doExecute(Collection<Record<Event>> records) {
        for (final Record<Event> record : records) {
            final Event event = record.getData();
            Object value = event.get(source, Object.class);
            if (value != null) {
                try {
                    event.put(target, objectMapper.writeValueAsString(value));
                } catch (Exception e) {
                    LOG.error("Failed to convert source to json string");
                }
            }
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
