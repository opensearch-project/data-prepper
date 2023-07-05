/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.keyvalue;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

@DataPrepperPlugin(name = "key_value", pluginType = Processor.class, pluginConfigurationType = KeyValueProcessorConfig.class)
public class KeyValueProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(KeyValueProcessor.class);

    private final KeyValueProcessorConfig keyValueProcessorConfig;

    private final Pattern fieldDelimiterPattern;
    private final Pattern keyValueDelimiterPattern;
    private final Set<String> includeKeysSet = new HashSet<String>();
    private final Set<String> validTransformOptionSet = Set.of("", "lowercase", "uppercase", "capitalize");
    private final String LOWERCASE_KEY = "lowercase";
    private final String UPPERCASE_KEY = "uppercase";
    private final String CAPITALIZE_KEY = "capitalize";

    @DataPrepperPluginConstructor
    public KeyValueProcessor(final PluginMetrics pluginMetrics, final KeyValueProcessorConfig keyValueProcessorConfig) {
        super(pluginMetrics);
        this.keyValueProcessorConfig = keyValueProcessorConfig;

        if(keyValueProcessorConfig.getFieldDelimiterRegex() != null
                && !keyValueProcessorConfig.getFieldDelimiterRegex().isEmpty()) {
            if(keyValueProcessorConfig.getFieldSplitCharacters() != null
                && !keyValueProcessorConfig.getFieldSplitCharacters().isEmpty()) {
                throw new IllegalArgumentException("field_delimiter_regex and field_split_characters cannot both be defined.");
            } else if(!validateRegex(keyValueProcessorConfig.getFieldDelimiterRegex())) {
                throw new PatternSyntaxException("field_delimiter_regex is not a valid regex string", keyValueProcessorConfig.getFieldDelimiterRegex(), -1);
            }

            fieldDelimiterPattern = Pattern.compile(keyValueProcessorConfig.getFieldDelimiterRegex());
        } else {
            String regex;
            if (keyValueProcessorConfig.getFieldSplitCharacters().isEmpty()) {
                regex = KeyValueProcessorConfig.DEFAULT_FIELD_SPLIT_CHARACTERS;
            } else {
                regex = buildRegexFromCharacters(keyValueProcessorConfig.getFieldSplitCharacters());
            }

            fieldDelimiterPattern = Pattern.compile(regex);
        }

        if(keyValueProcessorConfig.getKeyValueDelimiterRegex() != null
                && !keyValueProcessorConfig.getKeyValueDelimiterRegex().isEmpty()) {
            if(keyValueProcessorConfig.getValueSplitCharacters() != null
                && !keyValueProcessorConfig.getValueSplitCharacters().isEmpty()) {
                throw new IllegalArgumentException("key_value_delimiter_regex and value_split_characters cannot both be defined.");
            } else if (!validateRegex(keyValueProcessorConfig.getKeyValueDelimiterRegex())) {
                throw new PatternSyntaxException("key_value_delimiter_regex is not a valid regex string", keyValueProcessorConfig.getKeyValueDelimiterRegex(), -1);
            }

            keyValueDelimiterPattern = Pattern.compile(keyValueProcessorConfig.getKeyValueDelimiterRegex());
        } else {
            String regex;
            if(keyValueProcessorConfig.getValueSplitCharacters().isEmpty()) {
                regex = KeyValueProcessorConfig.DEFAULT_VALUE_SPLIT_CHARACTERS;
            } else {
                regex = buildRegexFromCharacters(keyValueProcessorConfig.getValueSplitCharacters());
            }

            keyValueDelimiterPattern = Pattern.compile(regex);
        }

        if(!validateRegex(keyValueProcessorConfig.getDeleteKeyRegex())) {
            throw new PatternSyntaxException("delete_key_regex is not a valid regex string", keyValueProcessorConfig.getDeleteKeyRegex(), -1);
        }

        if(!validateRegex(keyValueProcessorConfig.getDeleteValueRegex())) {
            throw new PatternSyntaxException("delete_value_regex is not a valid regex string", keyValueProcessorConfig.getDeleteValueRegex(), -1);
        }

        if(keyValueProcessorConfig.getIncludeKeys() != null) {
            includeKeysSet.addAll(keyValueProcessorConfig.getIncludeKeys());
        }

        if(!validTransformOptionSet.contains(keyValueProcessorConfig.getTransformKey())) {
            throw new IllegalArgumentException(String.format("The transform_key value: %s is not a valid option", keyValueProcessorConfig.getTransformKey()));
        }
    }

    private String buildRegexFromCharacters(String s) {
        char[] splitters = s.toCharArray();
        StringBuilder regexedFieldSplitCharacters = new StringBuilder();
        for(char c : splitters) {
            if(Objects.equals(c, '\\')) {
                regexedFieldSplitCharacters.append(c);
            } else {
                regexedFieldSplitCharacters.append(c).append('|');
            }
        }

        regexedFieldSplitCharacters = new StringBuilder(regexedFieldSplitCharacters.substring(0, regexedFieldSplitCharacters.length() - 1));

        return regexedFieldSplitCharacters.toString();
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

                if (!includeKeysSet.isEmpty() && !includeKeysSet.contains(key)) {
                    LOG.debug(String.format("Skipping not included key: '%s'", key));
                    continue;
                }

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

                if(keyValueProcessorConfig.getTransformKey() != null
                        && !keyValueProcessorConfig.getTransformKey().isEmpty()) {
                    key = transformKey(key);
                }

                addKeyValueToMap(parsedMap, key, value);
            }

            recordEvent.put(keyValueProcessorConfig.getDestination(), parsedMap);
        }

        return records;
    }

    private String transformKey(String key) {
        if(keyValueProcessorConfig.getTransformKey().equals(LOWERCASE_KEY)) {
            key = key.toLowerCase();
        } else if(keyValueProcessorConfig.getTransformKey().equals(UPPERCASE_KEY)) {
            key = key.substring(0, 1).toUpperCase() + key.substring(1);
        } else if(keyValueProcessorConfig.getTransformKey().equals(CAPITALIZE_KEY)) {
            key = key.toUpperCase();
        }
        return key;
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
