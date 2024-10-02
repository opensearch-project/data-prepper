/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.EVENT;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A simple String implementation of {@link Processor} which generates new Records with uppercase or lowercase content. The current
 * simpler implementation does not handle errors (if any).
 */
@DataPrepperPlugin(name = "string_converter", pluginType = Processor.class, pluginConfigurationType = StringProcessor.Configuration.class)
public class StringProcessor implements Processor<Record<Event>, Record<Event>> {
    private static Logger LOG = LoggerFactory.getLogger(StringProcessor.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final TypeReference<Map<String, Object>> mapTypeReference = new TypeReference<Map<String, Object>>() {};

    public static final String UPPER_CASE = "upper_case";

    private final boolean upperCase;

    @JsonPropertyOrder
    @JsonClassDescription("The `string_converter` processor converts a string to uppercase or lowercase.")
    public static class Configuration {
        @JsonPropertyDescription("Whether to convert to uppercase (<code>true</code>) or lowercase (<code>false</code>).")
        private boolean upperCase = true;

        public boolean getUpperCase() {
            return upperCase;
        }

        public void setUpperCase(final boolean upperCase) {
            this.upperCase = upperCase;
        }
    }

    /**
     * Mandatory constructor for Data Prepper Component - This constructor is used by Data Prepper
     * runtime engine to construct an instance of {@link StringProcessor} using an instance of {@link PluginSetting} which
     * has access to pluginSetting metadata from pipeline
     * pluginSetting file.
     *
     * @param configuration instance with metadata information from pipeline pluginSetting file.
     */
    @DataPrepperPluginConstructor
    public StringProcessor(final Configuration configuration) {
        this.upperCase = configuration.getUpperCase();
    }

    @Override
    public Collection<Record<Event>> execute(final Collection<Record<Event>> records) {
        final Collection<Record<Event>> modifiedRecords = new ArrayList<>(records.size());
        for (Record<Event> record : records) {
            final Event recordEvent = record.getData();
            final String eventJson = recordEvent.toJsonString();
            try {
                final Map<String, Object> newData = processEventJson(eventJson);
                final Event newRecordEvent = JacksonEvent.builder()
                        .withEventMetadata(recordEvent.getMetadata())
                        .withData(newData)
                        .build();
                modifiedRecords.add(new Record<>(newRecordEvent));
            } catch (JsonProcessingException e) {
                LOG.atError()
                        .addMarker(EVENT)
                        .addMarker(NOISY)
                        .setMessage("Unable to process Event data: {}")
                        .addArgument(eventJson)
                        .setCause(e)
                        .log();
            }
        }
        return modifiedRecords;
    }

    private Map<String, Object> processEventJson(final String data) throws JsonProcessingException {
        final Map<String, Object> dataMap = objectMapper.readValue(data, mapTypeReference);
        return dataMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                    final Object val = entry.getValue();
                    if (val instanceof String) {
                        return upperCase? ((String) val).toUpperCase() : ((String) val).toLowerCase();
                    } else {
                        return val;
                    }
                }));
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
