/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.prepper;

import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.annotations.DataPrepperPluginConstructor;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.JacksonEvent;
import com.amazon.dataprepper.model.prepper.Prepper;
import com.amazon.dataprepper.model.record.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * An simple String implementation of {@link Prepper} which generates new Records with upper case or lowercase content. The current
 * simpler implementation does not handle errors (if any).
 */
@DataPrepperPlugin(name = "string_converter", pluginType = Prepper.class, pluginConfigurationType = StringPrepper.Configuration.class)
public class StringPrepper implements Prepper<Record<Event>, Record<Event>> {
    private static Logger LOG = LoggerFactory.getLogger(StringPrepper.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final TypeReference<Map<String, Object>> mapTypeReference = new TypeReference<Map<String, Object>>() {};

    public static final String UPPER_CASE = "upper_case";

    private final boolean upperCase;

    public static class Configuration {
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
     * runtime engine to construct an instance of {@link StringPrepper} using an instance of {@link PluginSetting} which
     * has access to pluginSetting metadata from pipeline
     * pluginSetting file.
     *
     * @param configuration instance with metadata information from pipeline pluginSetting file.
     */
    @DataPrepperPluginConstructor
    public StringPrepper(final Configuration configuration) {
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
                LOG.error("Unable to process Event data: {}", eventJson, e);
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
