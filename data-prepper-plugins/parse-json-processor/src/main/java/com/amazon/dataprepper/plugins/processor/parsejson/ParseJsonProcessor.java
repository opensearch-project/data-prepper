/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.parsejson;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.annotations.DataPrepperPluginConstructor;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.processor.AbstractProcessor;
import com.amazon.dataprepper.model.processor.Processor;
import com.amazon.dataprepper.model.record.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@DataPrepperPlugin(name = "parse_json", pluginType = Processor.class, pluginConfigurationType = ParseJsonProcessorConfig.class)
public class ParseJsonProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(ParseJsonProcessor.class);

    private final String source;
    private final String destination;
    @DataPrepperPluginConstructor
    public ParseJsonProcessor(final PluginMetrics pluginMetrics, final ParseJsonProcessorConfig parseJsonProcessorConfig) {
        super(pluginMetrics);

        source = parseJsonProcessorConfig.getSource();
        destination = parseJsonProcessorConfig.getDestination();
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        final ObjectMapper objectMapper = new ObjectMapper();
        final boolean doWriteToRoot = Objects.isNull(destination);

        for (final Record<Event> record : records) {
            final Event event = record.getData();
            final String message = event.get(source, String.class);
            try {
                TypeReference<HashMap<String, Object>> hashMapTypeReference = new TypeReference<HashMap<String, Object>>() {};
                final Map<String, Object> parsedJson = objectMapper.readValue(message, hashMapTypeReference);

                if (doWriteToRoot) {
                    writeToRoot(event, parsedJson);
                } else {
                    event.put(destination, parsedJson);
                }
            } catch (final JsonProcessingException jsonException) {
                LOG.error("An exception occurred due to invalid JSON while reading event [{}]", event, jsonException);
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

    private void writeToRoot(final Event event, final Map<String, Object> parsedJson) {
        for (Map.Entry<String, Object> entry : parsedJson.entrySet()) {
            event.put(entry.getKey(), entry.getValue());
        }
    }
}
