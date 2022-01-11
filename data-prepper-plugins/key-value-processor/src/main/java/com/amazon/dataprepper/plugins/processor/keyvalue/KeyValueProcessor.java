/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.keyvalue;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.annotations.DataPrepperPluginConstructor;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.processor.AbstractProcessor;
import com.amazon.dataprepper.model.processor.Processor;
import com.amazon.dataprepper.model.record.Record;
import com.sun.tools.javac.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

@DataPrepperPlugin(name = "kv", pluginType = Processor.class, pluginConfigurationType = KeyValueProcessorConfig.class)
public class KeyValueProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(KeyValueProcessor.class);

    private final KeyValueProcessorConfig keyValueProcessorConfig;

    @DataPrepperPluginConstructor
    public KeyValueProcessor(final PluginMetrics pluginMetrics, final KeyValueProcessorConfig keyValueProcessorConfig) {
        super(pluginMetrics);
        this.keyValueProcessorConfig = keyValueProcessorConfig;
    }

    @Override
    public Collection<Record<Event>> doExecute(Collection<Record<Event>> records) {
        for(final Record<Event> record : records) {
            final Map<String, Object> parsedMap = new TreeMap<>();
            final Event recordEvent = record.getData();

            final String groupsRaw = recordEvent.get(keyValueProcessorConfig.getSource(), String.class);
            final String[] groups = groupsRaw.split(keyValueProcessorConfig.getFieldDelimiterRegex(), 0);
            for(final String group : groups) {
                final String[] terms = group.split(keyValueProcessorConfig.getKeyValueDelimiterRegex(), 2);
                String key = terms[0];
                String value;

                if(keyValueProcessorConfig.getTrimKeyRegex() != null && !Objects.equals(keyValueProcessorConfig.getTrimKeyRegex(), "")) {
                    key = key.replaceAll(keyValueProcessorConfig.getTrimKeyRegex(), "");
                }
                key = keyValueProcessorConfig.getPrefix() + key;

                //Expected number of terms to be produced
                if (terms.length == 2) {
                    value = terms[1];
                } else {
                    LOG.info(String.format("Unsuccessful match: '%s'", terms[0]));
                    value = keyValueProcessorConfig.getNonMatchValue();
                }

                if(keyValueProcessorConfig.getTrimValueRegex() != null && !Objects.equals(keyValueProcessorConfig.getTrimValueRegex(), "")) {
                    value = value.replaceAll(keyValueProcessorConfig.getTrimValueRegex(), "");
                }

                //When enabled, if the parsedMap already has the key, then convert its value from a string to a
                //LinkedList of strings including the original string plus the newly added string;
                //otherwise add in the new key/value
                if (keyValueProcessorConfig.getAllowDuplicateValues() && parsedMap.containsKey(key)) {
                    final Object existentValue = parsedMap.get(key);
                    if (existentValue.getClass() == String.class) {
                        LinkedList<String> list = new LinkedList<>(List.of((String) existentValue, value));
                        parsedMap.replace(key, list);
                    } else {
                        ((LinkedList<String>) existentValue).add(value);
                    }
                } else {
                    parsedMap.put(key, value);
                }
            }

            recordEvent.put(keyValueProcessorConfig.getDestination(), parsedMap);
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
