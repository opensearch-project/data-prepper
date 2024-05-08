/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.write_json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.plugin.PluginFactory;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.EVENT;
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
    private static final String WRITE_JSON_FAILED_COUNTER = "writeJsonFailedCounter";
    private final String source;
    private final Counter writeJsonFailedCounter;
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
        writeJsonFailedCounter = pluginMetrics.counter(WRITE_JSON_FAILED_COUNTER);
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
                    LOG.error(EVENT, "Failed to convert source to json string", e);
                    writeJsonFailedCounter.increment();
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
