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
                Object value;

                if(keyValueProcessorConfig.getTrimKeyRegex() != null && !Objects.equals(keyValueProcessorConfig.getTrimKeyRegex(), "")) {
                    key = key.replaceAll(keyValueProcessorConfig.getTrimKeyRegex(), "");
                }
                key = keyValueProcessorConfig.getPrefix() + key;

                if (terms.length == 2) {
                    value = terms[1];
                } else {
                    LOG.debug(String.format("Unsuccessful match: '%s'", terms[0]));
                    value = keyValueProcessorConfig.getNonMatchValue();
                }

                if(keyValueProcessorConfig.getTrimValueRegex() != null && !Objects.equals(keyValueProcessorConfig.getTrimValueRegex(), "")) {
                    value = ((String)value).replaceAll(keyValueProcessorConfig.getTrimValueRegex(), "");
                }

                addKeyValueToMap(parsedMap, key, value);
            }

            recordEvent.put(keyValueProcessorConfig.getDestination(), parsedMap);
        }

        return records;
    }

    private void addKeyValueToMap(Map<String, Object> parsedMap, String key, Object value) {
        if (parsedMap.containsKey(key)) {
            final Object existentValue = parsedMap.get(key);
            if (existentValue instanceof String) {
                LinkedList<Object> list = new LinkedList<>();
                list.add(existentValue);
                list.add(value);

                parsedMap.replace(key, list);
            } else {
                ((LinkedList<Object>) existentValue).add(value);
            }
        } else {
            parsedMap.put(key, value);
        }
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
