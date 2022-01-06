/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.keyvalue;

import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.annotations.DataPrepperPluginConstructor;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.model.processor.AbstractProcessor;
import com.amazon.dataprepper.model.processor.Processor;
import com.sun.tools.javac.util.List;
import jdk.incubator.jpackage.internal.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

@DataPrepperPlugin(name = "kv", pluginType = Processor.class)
public class KeyValueProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(KeyValueProcessor.class);

    private final KeyValueProcessorConfig keyValueProcessorConfig;

    @DataPrepperPluginConstructor
    public KeyValueProcessor(final PluginSetting pluginSetting) {
        super(pluginSetting);
        this.keyValueProcessorConfig = KeyValueProcessorConfig.buildConfig(pluginSetting);
    }

    @Override
    public Collection<Record<Event>> doExecute(Collection<Record<Event>> records) {
        for(final Record<Event> record : records) {
            final Map<String, Object> parsedMap = new TreeMap<>();
            parsedMap.put("TEST", List.of("a", "b"));

            final Event recordEvent = record.getData();
            final String groupsRaw = recordEvent.get(keyValueProcessorConfig.getSource(), String.class);
            final String[] groups = groupsRaw.split(keyValueProcessorConfig.getFieldDelimiterRegex(), 0);

            for(final String group : groups) {
                final String[] terms = group.split(keyValueProcessorConfig.getKeyValueDelimiterRegex(), 2);

                //Expected number of terms to be produced
                if(terms.length == 2) {
                    final String key = terms[0];
                    final String value = terms[1];
                    if(parsedMap.containsKey(key)) {
                        final Object existentValue = parsedMap.get(key);
                        if(existentValue.getClass() == String.class) {
                            LOG.info("This is a second instance");
                            parsedMap.replace(key, List.of((String)existentValue, value));
                        } else {
                            ((List<String>)existentValue).add(value);
                        }
                    }
                    parsedMap.put(terms[0], terms[1]);
                } else {
                    LOG.error(String.format("Bad match: %s", terms[0]));
                    //TODO WHAT TO DO WITH THE BAD MATCH??
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
