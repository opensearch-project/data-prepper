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
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.core.type.TypeReference;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.regex.Matcher;
import java.util.Stack;
import java.util.ArrayList;

@DataPrepperPlugin(name = "key_value", pluginType = Processor.class, pluginConfigurationType = KeyValueProcessorConfig.class)
public class KeyValueProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(KeyValueProcessor.class);

    private final KeyValueProcessorConfig keyValueProcessorConfig;

    private final Pattern fieldDelimiterPattern;
    private final Pattern keyValueDelimiterPattern;
    private final Set<String> includeKeysSet = new HashSet<String>();
    private final Set<String> excludeKeysSet = new HashSet<String>();
    private final HashMap<String, Object> defaultValuesMap = new HashMap<>();
    private final Set<String> defaultValuesSet = new HashSet<String>();
    private final String lowercaseKey = "lowercase";
    private final String uppercaseKey = "uppercase";
    private final String capitalizeKey = "capitalize";
    private final Set<String> validTransformOptionSet = Set.of("", lowercaseKey, uppercaseKey, capitalizeKey);
    private final String whitespaceStrict = "strict";
    private final String whitespaceLenient = "lenient";
    private final Set<String> validWhitespaceSet = Set.of(whitespaceLenient, whitespaceStrict);
    private static HashMap<Character, Character> bracketMap = new HashMap<>();
    private HashMap<String, Object> nonRecursedMap = new LinkedHashMap<>();
    private HashMap<String, Object> recursedMap = new LinkedHashMap<>();

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

        if (!validateRegex(keyValueProcessorConfig.getDeleteKeyRegex())) {
            throw new PatternSyntaxException("delete_key_regex is not a valid regex string", keyValueProcessorConfig.getDeleteKeyRegex(), -1);
        }

        if (!validateRegex(keyValueProcessorConfig.getDeleteValueRegex())) {
            throw new PatternSyntaxException("delete_value_regex is not a valid regex string", keyValueProcessorConfig.getDeleteValueRegex(), -1);
        }

        includeKeysSet.addAll(keyValueProcessorConfig.getIncludeKeys());
        excludeKeysSet.addAll(keyValueProcessorConfig.getExcludeKeys());
        defaultValuesMap.putAll(keyValueProcessorConfig.getDefaultValues());
        if (!defaultValuesMap.isEmpty()) {
            defaultValuesSet.addAll(defaultValuesMap.keySet());
        }

        validateKeySets(includeKeysSet, excludeKeysSet, defaultValuesSet);
        
        if (!validTransformOptionSet.contains(keyValueProcessorConfig.getTransformKey())) {
            throw new IllegalArgumentException(String.format("The transform_key value: %s is not a valid option", keyValueProcessorConfig.getTransformKey()));
        }

        if (!(validWhitespaceSet.contains(keyValueProcessorConfig.getWhitespace()))) {
            throw new IllegalArgumentException(String.format("The whitespace value: %s is not a valid option", keyValueProcessorConfig.getWhitespace()));
        }

        final Pattern boolCheck = Pattern.compile("true|false", Pattern.CASE_INSENSITIVE);
        final Matcher duplicateValueBoolMatch = boolCheck.matcher(String.valueOf(keyValueProcessorConfig.getSkipDuplicateValues()));
        final Matcher removeBracketsBoolMatch = boolCheck.matcher(String.valueOf(keyValueProcessorConfig.getRemoveBrackets()));
        final Matcher recursiveBoolMatch = boolCheck.matcher(String.valueOf(keyValueProcessorConfig.getRecursive()));

        if (!duplicateValueBoolMatch.matches()) {
            throw new IllegalArgumentException(String.format("The skip_duplicate_values value must be either true or false", keyValueProcessorConfig.getSkipDuplicateValues()));
        }

        if (!removeBracketsBoolMatch.matches()) {
            throw new IllegalArgumentException(String.format("The remove_brackets value must be either true or false", keyValueProcessorConfig.getRemoveBrackets()));
        }

        if (!recursiveBoolMatch.matches()) {
            throw new IllegalArgumentException(String.format("The recursive value must be either true or false", keyValueProcessorConfig.getRemoveBrackets()));
        }

        if (keyValueProcessorConfig.getRemoveBrackets() && keyValueProcessorConfig.getRecursive()) {
            throw new IllegalArgumentException("Cannot remove brackets needed for determining levels of recursion");
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

    private void validateKeySets(final Set<String> includeSet, final Set<String> excludeSet, final Set<String> defaultSet) {
        final Set<String> includeIntersectionSet = new HashSet<String>(includeSet);
        final Set<String> defaultIntersectionSet = new HashSet<String>(defaultSet);

        includeIntersectionSet.retainAll(excludeSet);
        if (!includeIntersectionSet.isEmpty()) {
            throw new IllegalArgumentException("Include keys and exclude keys set cannot have any overlap");
        }

        defaultIntersectionSet.retainAll(excludeSet);
        if (!defaultIntersectionSet.isEmpty()) {
            throw new IllegalArgumentException("Cannot exclude a default key!");
        }
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for(final Record<Event> record : records) {
            final Map<String, Object> parsedMap = new HashMap<>();
            final Event recordEvent = record.getData();

            final String groupsRaw = recordEvent.get(keyValueProcessorConfig.getSource(), String.class);

            if (keyValueProcessorConfig.getRecursive()) {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode recursedTree = recurse(groupsRaw, mapper);
                createRecursedMap(recursedTree, mapper);
                executeConfigs(recursedMap, parsedMap);
            } else {
                final String[] groups = fieldDelimiterPattern.split(groupsRaw, 0);
                createNonRecursedMap(groups);
                executeConfigs(nonRecursedMap, parsedMap);
            }

            for (Map.Entry<String,Object> pair : defaultValuesMap.entrySet()) {
                if (parsedMap.containsKey(pair.getKey())) {
                    LOG.debug("Skipping already included default key: '{}'", pair.getKey());
                    continue;
                }
                parsedMap.put(pair.getKey(), pair.getValue());
            }

            recordEvent.put(keyValueProcessorConfig.getDestination(), parsedMap);
        }

        return records;
    }

    private ObjectNode recurse(String input, ObjectMapper mapper) {
        Stack<Character> bracketStack = new Stack<Character>();
        initBracketMap();
        int pairStart = 0;

        ArrayList<String> pairs = new ArrayList<String>();

        // create empty root node
        ObjectNode root = mapper.createObjectNode();

        for (int i = 0; i < input.length(); i++) {
            if (bracketMap.containsKey(input.charAt(i))) { // open bracket
                bracketStack.push(input.charAt(i));
            }

            if (bracketMap.containsValue(input.charAt(i)) && !bracketStack.isEmpty()) { // closed bracket
                if (bracketMap.get(bracketStack.peek()) == input.charAt(i)) { // check if brackets are matched
                    bracketStack.pop();
                }
            }

            if (bracketStack.isEmpty() && input.charAt(i) == '&') { // config variable
                // save pairs in array
                String pair = input.substring(pairStart, i - 1);
                pairs.add(pair);
                pairStart = i + 1;
            }
        }

        // handle last pair case after parsing thru input and there are no more splitters
        int pairEnd = input.length();
        pairs.add(input.substring(pairStart, pairEnd - 1));

        for (final String pair : pairs) {
            int keyStart = 0;
            int keyEnd = -1;
            int valueStart = -1;
            int valueEnd = -1;
            String keyString = "";
            String valueString;

            bracketStack.clear();

            for (int i = 0; i < pair.length(); i++) { // search for kv splitter
                if (bracketStack.isEmpty() && pair.charAt(i) == '=') { // change to config variable
                    keyString = pair.substring(keyStart, i - 1);
                    valueStart = i + 1;
                    break;
                }
            }

            if (keyString.isBlank()) {
                // handle nonmatch value
                keyString = pair;
                LOG.debug("Unsuccessful match: '{}'", keyString);
                valueString = keyValueProcessorConfig.getNonMatchValue().toString();
            } else if (bracketMap.containsKey(pair.charAt(valueStart))) { // nested content
                bracketStack.push(pair.charAt(valueStart));
                valueStart++;

                for (int i = valueStart + 1; i < pair.length(); i++) {
                    if (bracketMap.containsValue(pair.charAt(i))) {
                        if (bracketMap.get(bracketStack.peek()) == pair.charAt(i)) { // brackets match, set up for recursion
                            valueEnd = i;
                            bracketStack.pop();
                            valueString = pair.substring(valueStart, valueEnd - 1);
                            JsonNode child = ((ObjectNode) root).put(keyString, recurse(valueString, mapper));
                        }
                    }
                }  
            } else { // no nested content
                valueEnd = pair.length();
                valueString = pair.substring(valueStart, valueEnd - 1);

                ObjectNode child = ((ObjectNode)root).put(keyString, valueString);
            }
        }

        return root;
    }

    private static void initBracketMap() {
        bracketMap.put('[', ']');
        bracketMap.put('(', ')');
        bracketMap.put('<', '>');
    }

    private void createRecursedMap(JsonNode node, ObjectMapper mapper) {
        recursedMap = mapper.convertValue(node, new TypeReference<HashMap<String, Object>>() {});
    }

    private void createNonRecursedMap(String[] groups) {
        for(final String group : groups) {
            final String[] terms = keyValueDelimiterPattern.split(group, 2);
            String key = terms[0];
            Object value;
            
            if (terms.length == 2) {
                value = terms[1];
            } else {
                LOG.debug("Unsuccessful match: '{}'", terms[0]);
                value = keyValueProcessorConfig.getNonMatchValue();
            }

            nonRecursedMap.put(key, value);
        }
    }

    private void executeConfigs(HashMap<String, Object> map, Map<String, Object> parsed) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            if (!includeKeysSet.isEmpty() && !includeKeysSet.contains(key)) {
                LOG.debug("Skipping not included key: '{}'", key);
                continue;
            }

            if (excludeKeysSet.contains(key)) {
                LOG.debug("Key is being excluded: '{}'", key);
                continue;
            }

            if(keyValueProcessorConfig.getDeleteKeyRegex() != null && !Objects.equals(keyValueProcessorConfig.getDeleteKeyRegex(), "")) {
                key = key.replaceAll(keyValueProcessorConfig.getDeleteKeyRegex(), "");
            }
            key = keyValueProcessorConfig.getPrefix() + key;

            if(value != null
                    && value instanceof String
                    && keyValueProcessorConfig.getDeleteValueRegex() != null
                    && !Objects.equals(keyValueProcessorConfig.getDeleteValueRegex(), "")) {
                value = ((String)value).replaceAll(keyValueProcessorConfig.getDeleteValueRegex(), "");
            }

            if (keyValueProcessorConfig.getWhitespace().equals(whitespaceStrict)) {
                String[] whitespace_arr = trimWhitespace(key, value);
                key = whitespace_arr[0];
                value = whitespace_arr[1];
            }

            if (keyValueProcessorConfig.getTransformKey() != null
                    && !keyValueProcessorConfig.getTransformKey().isEmpty()) {
                key = transformKey(key);
            }

            if (keyValueProcessorConfig.getRemoveBrackets()) {
                final String bracketRegex = "[\\[\\]()<>]";
                if (value != null) {
                    value = value.toString().replaceAll(bracketRegex,"");
                }
            }

            addKeyValueToMap(parsed, key, value);
        }
    }

    private Map<String, Object> treeToMap(ObjectMapper mapper, JsonNode root) {
        Map<String, Object> map = mapper.convertValue(root, Map.class);
        return map;
    }

    private String[] trimWhitespace(String key, Object value) {
        String[] arr = {key.stripTrailing(), value.toString().stripLeading()};
        return arr;
    }
    
    private String transformKey(String key) {
        if (keyValueProcessorConfig.getTransformKey().equals(lowercaseKey)) {
            key = key.toLowerCase();
        } else if (keyValueProcessorConfig.getTransformKey().equals(uppercaseKey)) {
            key = key.substring(0, 1).toUpperCase() + key.substring(1);
        } else if (keyValueProcessorConfig.getTransformKey().equals(capitalizeKey)) {
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
            if (keyValueProcessorConfig.getSkipDuplicateValues()) {
                if (((List<Object>) parsedMap.get(key)).contains(value)) {
                    return;
                }
            }

            ((List<Object>) parsedMap.get(key)).add(value);
        } else {
            if (keyValueProcessorConfig.getSkipDuplicateValues()) {
                if (parsedMap.containsValue(value)) {
                    return;
                }
            }

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
