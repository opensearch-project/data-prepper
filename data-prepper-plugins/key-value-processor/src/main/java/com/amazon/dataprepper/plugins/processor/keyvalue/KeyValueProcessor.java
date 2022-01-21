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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

@DataPrepperPlugin(name = "key_value", pluginType = Processor.class, pluginConfigurationType = KeyValueProcessorConfig.class)
public class KeyValueProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(KeyValueProcessor.class);

    private final KeyValueProcessorConfig keyValueProcessorConfig;

    private Pattern fieldDelimiterPattern = null;
    private final Pattern keyValueDelimiterPattern;

    @DataPrepperPluginConstructor
    public KeyValueProcessor(final PluginMetrics pluginMetrics, final KeyValueProcessorConfig keyValueProcessorConfig) throws PatternSyntaxException {
        super(pluginMetrics);
        this.keyValueProcessorConfig = keyValueProcessorConfig;

        if(keyValueProcessorConfig.getFieldDelimiterRegex() != null
            && !Objects.equals(keyValueProcessorConfig.getFieldDelimiterRegex(), "")) {
            if(!validateRegex(keyValueProcessorConfig.getFieldDelimiterRegex())) {
                throw new PatternSyntaxException("field_delimiter_regex is not a valid regex string", keyValueProcessorConfig.getFieldDelimiterRegex(), -1);
            }

            fieldDelimiterPattern = Pattern.compile(keyValueProcessorConfig.getFieldDelimiterRegex());
        } else {
            char[] splitters = keyValueProcessorConfig.getFieldSplitCharacters().toCharArray();
            StringBuilder regexedFieldSplitCharacters = new StringBuilder();
            for(char c : splitters) {
                if(Objects.equals(c, '\\')) {
                    regexedFieldSplitCharacters.append(c);
                } else {
                    regexedFieldSplitCharacters.append(c).append('|');
                }
            }

            regexedFieldSplitCharacters = new StringBuilder(regexedFieldSplitCharacters.substring(0, regexedFieldSplitCharacters.length() - 1));
            fieldDelimiterPattern = Pattern.compile(regexedFieldSplitCharacters.toString());
        }

        if(!validateRegex(keyValueProcessorConfig.getKeyValueDelimiterRegex())) {
            throw new PatternSyntaxException("key_value_delimiter_regex is not a valid regex string", keyValueProcessorConfig.getKeyValueDelimiterRegex(), -1);
        } else {
            keyValueDelimiterPattern = Pattern.compile(keyValueProcessorConfig.getKeyValueDelimiterRegex());
        }

        if(!validateRegex(keyValueProcessorConfig.getDeleteKeyRegex())) {
            throw new PatternSyntaxException("delete_key_regex is not a valid regex string", keyValueProcessorConfig.getDeleteKeyRegex(), -1);
        }

        if(!validateRegex(keyValueProcessorConfig.getDeleteValueRegex())) {
            throw new PatternSyntaxException("delete_value_regex is not a valid regex string", keyValueProcessorConfig.getDeleteValueRegex(), -1);
        }
    }

    private boolean validateRegex(final String pattern)
    {
        if(pattern != null && !Objects.equals(pattern, "")) {
            try {
                Pattern.compile(pattern);
            } catch (PatternSyntaxException e) {
                return false;
            }
        }

        return true;
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for(final Record<Event> record : records) {
            final Map<String, Object> parsedMap = new HashMap<>();
            final Event recordEvent = record.getData();

            final String groupsRaw = recordEvent.get(keyValueProcessorConfig.getSource(), String.class);
            final String[] groups = fieldDelimiterPattern.split(groupsRaw, 0);
            for(final String group : groups) {
                final String[] terms = keyValueDelimiterPattern.split(group, 2);
                String key = terms[0];
                Object value;

                if(keyValueProcessorConfig.getDeleteKeyRegex() != null && !Objects.equals(keyValueProcessorConfig.getDeleteKeyRegex(), "")) {
                    key = key.replaceAll(keyValueProcessorConfig.getDeleteKeyRegex(), "");
                }
                key = keyValueProcessorConfig.getPrefix() + key;

                if (terms.length == 2) {
                    value = terms[1];
                } else {
                    LOG.debug(String.format("Unsuccessful match: '%s'", terms[0]));
                    value = keyValueProcessorConfig.getNonMatchValue();
                }

                if(value != null
                        && value instanceof String
                        && keyValueProcessorConfig.getDeleteValueRegex() != null
                        && !Objects.equals(keyValueProcessorConfig.getDeleteValueRegex(), "")) {
                    value = ((String)value).replaceAll(keyValueProcessorConfig.getDeleteValueRegex(), "");
                }

                addKeyValueToMap(parsedMap, key, value);
            }

            recordEvent.put(keyValueProcessorConfig.getDestination(), parsedMap);
        }

        return records;
    }

    private void addKeyValueToMap(final Map<String, Object> parsedMap, final String key, final Object value) {
        if(!parsedMap.containsKey(key)) {
            parsedMap.put(key, value);
            return;
        }

        if (parsedMap.get(key) instanceof List) {
            ((List<Object>) parsedMap.get(key)).add(value);
        } else {
            final LinkedList<Object> combinedList = new LinkedList<>();
            combinedList.add(parsedMap.get(key));
            combinedList.add(value);

            parsedMap.replace(key, combinedList);
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
