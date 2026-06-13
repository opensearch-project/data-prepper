/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.keyvalue;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.common.TransformOption;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
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
import java.util.Stack;
import java.util.ArrayList;
import org.opensearch.dataprepper.model.pattern.Pattern;
import org.opensearch.dataprepper.model.pattern.PatternSyntaxException;
import org.opensearch.dataprepper.model.pattern.Matcher;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.EVENT;

@DataPrepperPlugin(name = "key_value", pluginType = Processor.class, pluginConfigurationType = KeyValueProcessorConfig.class)
public class KeyValueProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(KeyValueProcessor.class);
    private static String startGroupStrings[] = {"\"","'", "(", "[", "<", "{", "http://", "https://"};
    private static Character endGroupChars[] ={'"', '\'', ')', ']', '>', '}', ' ', ' '};

    private final KeyValueProcessorConfig keyValueProcessorConfig;

    private final ExpressionEvaluator expressionEvaluator;

    private final Pattern fieldDelimiterPattern;
    private final Pattern keyValueDelimiterPattern;
    private final Set<String> includeKeysSet = new HashSet<String>();
    private final Set<String> excludeKeysSet = new HashSet<String>();
    private final HashMap<String, Object> defaultValuesMap = new HashMap<>();
    private final Set<String> defaultValuesSet = new HashSet<String>();
    private final String whitespaceStrict = "strict";
    private final String whitespaceLenient = "lenient";
    private final Set<String> validWhitespaceSet = Set.of(whitespaceLenient, whitespaceStrict);
    final String delimiterBracketCheck = "[\\[\\]()<>]";
    private final Set<Character> bracketSet = Set.of('[', ']', '(', ')', '<', '>');
    private final List<String> tagsOnFailure;
    private final Character stringLiteralCharacter;
    private final String keyPrefix;
    private final boolean normalizeKeys;

    @DataPrepperPluginConstructor
    public KeyValueProcessor(final PluginMetrics pluginMetrics,
                             final KeyValueProcessorConfig keyValueProcessorConfig,
                             final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.keyValueProcessorConfig = keyValueProcessorConfig;

        this.stringLiteralCharacter = keyValueProcessorConfig.getStringLiteralCharacter();

        tagsOnFailure = keyValueProcessorConfig.getTagsOnFailure();

        this.normalizeKeys = keyValueProcessorConfig.getNormalizeKeys();

        if (keyValueProcessorConfig.getFieldDelimiterRegex() != null
                && !keyValueProcessorConfig.getFieldDelimiterRegex().isEmpty()) {
            if (keyValueProcessorConfig.getFieldSplitCharacters() != null
                && !keyValueProcessorConfig.getFieldSplitCharacters().isEmpty()) {
                throw new IllegalArgumentException("field_delimiter_regex and field_split_characters cannot both be defined.");
            } else if (!validateRegex(keyValueProcessorConfig.getFieldDelimiterRegex())) {
                throw new PatternSyntaxException("field_delimiter_regex is not a valid regex string", keyValueProcessorConfig.getFieldDelimiterRegex(), -1);
            }

            fieldDelimiterPattern = Pattern.compile(keyValueProcessorConfig.getFieldDelimiterRegex());

            if (keyValueProcessorConfig.getRecursive()) {
                if (fieldDelimiterPattern.matcher(delimiterBracketCheck).matches()) {
                    throw new IllegalArgumentException("While recursive is true, the set field delimiter regex cannot contain brackets while you are trying to recurse.");
                }
            }
        } else {
            String regex;
            if (keyValueProcessorConfig.getFieldSplitCharacters().isEmpty()) {
                regex = KeyValueProcessorConfig.DEFAULT_FIELD_SPLIT_CHARACTERS;
            } else {
                if (keyValueProcessorConfig.getRecursive()
                    && keyValueProcessorConfig.getFieldSplitCharacters().length() != 1) {
                    throw new IllegalArgumentException("While recursive is true, the set field split characters is limited to one character only.");
                }
                regex = buildRegexFromCharacters(keyValueProcessorConfig.getFieldSplitCharacters());
            }

            fieldDelimiterPattern = Pattern.compile(regex);

            if (keyValueProcessorConfig.getRecursive()
                && fieldDelimiterPattern.matcher(delimiterBracketCheck).matches()) {
                throw new IllegalArgumentException("While recursive is true, the set field delimiter cannot contain brackets while you are trying to recurse.");
            }
        }

        if (keyValueProcessorConfig.getKeyValueDelimiterRegex() != null
                && !keyValueProcessorConfig.getKeyValueDelimiterRegex().isEmpty()) {
            if (keyValueProcessorConfig.getValueSplitCharacters() != null
                && !keyValueProcessorConfig.getValueSplitCharacters().isEmpty()) {
                throw new IllegalArgumentException("key_value_delimiter_regex and value_split_characters cannot both be defined.");
            } else if (!validateRegex(keyValueProcessorConfig.getKeyValueDelimiterRegex())) {
                throw new PatternSyntaxException("key_value_delimiter_regex is not a valid regex string", keyValueProcessorConfig.getKeyValueDelimiterRegex(), -1);
            }

            keyValueDelimiterPattern = Pattern.compile(keyValueProcessorConfig.getKeyValueDelimiterRegex());

            if (keyValueProcessorConfig.getRecursive()
                && keyValueDelimiterPattern.matcher(delimiterBracketCheck).matches()) {
                throw new IllegalArgumentException("While recursive is true, the set key value delimiter regex cannot contain brackets while you are trying to recurse.");
            }
        } else {
            String regex;
            if (keyValueProcessorConfig.getValueSplitCharacters().isEmpty()) {
                regex = KeyValueProcessorConfig.DEFAULT_VALUE_SPLIT_CHARACTERS;
            } else {
                if (keyValueProcessorConfig.getRecursive()
                    && keyValueProcessorConfig.getValueSplitCharacters().length() != 1) {
                    throw new IllegalArgumentException("While recursive is true, the set value split characters is limited to one character only.");
                }

                regex = buildRegexFromCharacters(keyValueProcessorConfig.getValueSplitCharacters());
            }

            keyValueDelimiterPattern = Pattern.compile(regex);

            if (keyValueProcessorConfig.getRecursive()
                && keyValueDelimiterPattern.matcher(delimiterBracketCheck).matches()) {
                throw new IllegalArgumentException("While recursive is true, the set value split characters cannot contain brackets while you are trying to recurse.");
            }
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

        this.expressionEvaluator = expressionEvaluator;
        if (keyValueProcessorConfig.getKeyValueWhen() != null
                && !expressionEvaluator.isValidExpressionStatement(keyValueProcessorConfig.getKeyValueWhen())) {
            throw new InvalidPluginConfigurationException(
                    String.format("key_value_when %s is not a valid expression statement", keyValueProcessorConfig.getKeyValueWhen()));
        }

        keyPrefix = keyValueProcessorConfig.getPrefix() != null ? keyValueProcessorConfig.getPrefix() : "";
    }

    private String buildRegexFromCharacters(String s) {
        char[] splitters = s.toCharArray();
        StringBuilder regexedFieldSplitCharacters = new StringBuilder();
        for(char c : splitters) {
            if (Objects.equals(c, '\\')) {
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
        if (pattern != null && !Objects.equals(pattern, "")) {
            try {
                Pattern.compile(pattern);
            } catch (PatternSyntaxException e) {
                return false;
            }
        }

        return true;
    }

    private void validateKeySets(final Set<String> includeSet, final Set<String> excludeSet, final Set<String> defaultSet) {
        final Set<String> includeIntersectionSet = new HashSet<>(includeSet);
        final Set<String> defaultIntersectionSet = new HashSet<>(defaultSet);

        includeIntersectionSet.retainAll(excludeSet);
        if (!includeIntersectionSet.isEmpty()) {
            throw new IllegalArgumentException("Include keys and exclude keys set cannot have any overlap");
        }

        defaultIntersectionSet.retainAll(excludeSet);
        if (!defaultIntersectionSet.isEmpty()) {
            throw new IllegalArgumentException("Cannot exclude a default key!");
        }
    }

    private boolean isEscapedQuote(final String str, int idx) {
        if (idx > 0) {
            if ((str.charAt(idx) == '"' || str.charAt(idx) == '\'') &&
                (str.charAt(idx-1) == '\\'))
                return true;
        }
        return false;
    }

    public int skipGroup(final String str, int idx, final Character endChar) {
        int i = idx;
        while (i < str.length()) {
            if (isEscapedQuote(str, i)) {
                i++;
                continue;
            } else if (str.charAt(i) == endChar) {
                return i;
            } else
                i++;
        }
        if (keyValueProcessorConfig.isStrictGroupingEnabled()) {
            throw new RuntimeException("Bad Input, no end character found in "+str+" after index " + idx +", expected end char = "+endChar);
        }
        return i-1;
    }

    private void addPart(List<String> parts, final String str, final int start, final int end) {
        String part = str.substring(start,end).trim();
        if (part.length() > 0) {
            parts.add(part);
        }
    }

    private int findInStartGroup(final String str, final int idx) {
        if (idx < 0 || idx >= str.length()) {
            return -1; // Invalid starting index
        }

        for (int j = 0; j < startGroupStrings.length; j++) {
            String startGroup = startGroupStrings[j];
            int startGroupLen = startGroup.length();

            if (idx + startGroupLen <= str.length() && str.startsWith(startGroup, idx)) {
                // For the first two elements, check for escape characters
                if (j <= 1 && (idx == 0 || str.charAt(idx - 1) != '\\')) {
                    return j;
                } else if (j > 1) {
                    return j;
                }
            }
        }
        return -1;
    }

    private List<String> parseWithValueGrouping(String str) {
        String fieldDelimiter = keyValueProcessorConfig.getFieldSplitCharacters();
        Set<Character> fieldDelimiterSet = new HashSet<>();
        for (char ch : fieldDelimiter.toCharArray()) {
            fieldDelimiterSet.add(ch);
        }
        int i = 0;
        int start = i;
        List<String> parts = new ArrayList<>();
        while (i < str.length()) {
            // checks if a " or ' is escaped using \, if yes, it is
            // not considered as a start of a group
            if (isEscapedQuote(str, i)) {
                i++;
                continue;
            }

            int groupIndex = findInStartGroup(str, i);
            boolean skippedGroup = false;
            if (groupIndex >= 0) {
                String[] s = keyValueDelimiterPattern.split(str.substring(start,i+1));
                // Only handle Grouping patterns in the values, not keys
                if (s.length > 1 || (stringLiteralCharacter != null && startGroupStrings[groupIndex].charAt(0) == stringLiteralCharacter)) {
                    i = skipGroup(str, i+1, endGroupChars[groupIndex]);
                    skippedGroup = true;
                }
            }
            if (fieldDelimiterSet.contains(str.charAt(i))) {
                // If end of group character is same as field delimiter, then include that in the value if value grouping is done
                if (skippedGroup) {
                    i++;
                }
                addPart(parts, str, start, i);
                i++;
                start = i;
            } else {
               i++;
            }
        }
        if (start != i) {
            addPart(parts, str, start, i);
        }

        return parts;
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        final ObjectMapper mapper = new ObjectMapper();

        for (final Record<Event> record : records) {
            final Map<String, Object> outputMap = new HashMap<>();
            final Event recordEvent = record.getData();

            try {

                if (keyValueProcessorConfig.getKeyValueWhen() != null && !expressionEvaluator.evaluateConditional(keyValueProcessorConfig.getKeyValueWhen(), recordEvent)) {
                    continue;
                }

                final String groupsRaw = recordEvent.get(keyValueProcessorConfig.getSource(), String.class);
                if (groupsRaw == null) {
                    continue;
                }
                String[] groups;
                if (keyValueProcessorConfig.getValueGrouping()) {
                    groups = parseWithValueGrouping(groupsRaw).stream().toArray(String[]::new);
                } else {
                    groups = fieldDelimiterPattern.split(groupsRaw, 0);
                }

                if (keyValueProcessorConfig.getRecursive()) {
                    try {
                        JsonNode recursedTree = recurse(groupsRaw, mapper);
                        outputMap.putAll(createRecursedMap(recursedTree, mapper));
                    } catch (Exception e) {
                        LOG.error("Recursive parsing ran into an unexpected error, treating message as non-recursive", e);
                        recordEvent.getMetadata().addTags(tagsOnFailure);
                    }
                } else {
                    try {
                        outputMap.putAll(createNonRecursedMap(groups));
                    } catch (Exception e) {
                        LOG.error("Non-recursive parsing ran into an unexpected error", e);
                        recordEvent.getMetadata().addTags(tagsOnFailure);
                    }
                }

                final Map<String, Object> processedMap = executeConfigs(outputMap);

                if (Objects.isNull(keyValueProcessorConfig.getDestination())) {
                    writeToRoot(recordEvent, processedMap);
                } else {
                    if (keyValueProcessorConfig.getOverwriteIfDestinationExists() ||
                            !recordEvent.containsKey(keyValueProcessorConfig.getDestination())) {
                        recordEvent.put(keyValueProcessorConfig.getDestination(), processedMap, normalizeKeys);
                    }
                }
            } catch (final Exception e) {
                LOG.error(EVENT, "There was an exception while processing on Event [{}]: ", recordEvent, e);
                recordEvent.getMetadata().addTags(tagsOnFailure);
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

    private ObjectNode recurse(final String input, final ObjectMapper mapper) {
        Stack<Character> bracketStack = new Stack<>();
        Map<Character, Character> bracketMap = initBracketMap();
        int pairStart = 0;

        ArrayList<String> pairs = new ArrayList<>();
        ObjectNode root = mapper.createObjectNode();

        for (int i = 0; i < input.length(); i++) {
            if (bracketMap.containsKey(input.charAt(i))) {
                bracketStack.push(input.charAt(i));
            }

            if (bracketMap.containsValue(input.charAt(i)) && !bracketStack.isEmpty()) {
                if (bracketMap.get(bracketStack.peek()) == input.charAt(i)) {
                    bracketStack.pop();
                }
            }

            if (bracketStack.isEmpty() && fieldDelimiterPattern.matcher(String.valueOf(input.charAt(i))).matches()) {
                String pair = input.substring(pairStart, i);
                pairs.add(pair);
                pairStart = i + 1;
            }
        }

        pairs.add(input.substring(pairStart));

        for (final String pair : pairs) {
            int keyStart = 0;
            int keyEnd = -1;
            int valueStart = -1;
            int valueEnd = -1;
            String keyString = "";
            String valueString;
            final Character whitespaceChar = ' ';

            bracketStack.clear();

            for (int i = 0; i < pair.length(); i++) {
                if (bracketStack.isEmpty() && keyValueDelimiterPattern.matcher(String.valueOf(pair.charAt(i))).matches()) {
                    keyString = pair.substring(keyStart, i).stripTrailing();
                    valueStart = i + 1;
                    while(pair.charAt(valueStart) == whitespaceChar) {
                        valueStart++;
                    }
                    break;
                }
            }

            if (keyString.isBlank()) {
                keyString = pair;
                LOG.debug("Unsuccessful match: '{}'", keyString);
                valueString = keyValueProcessorConfig.getNonMatchValue().toString().stripLeading();
            } else if (bracketMap.containsKey(pair.charAt(valueStart))) {
                if (pair.charAt(pair.length() - 1) == bracketMap.get(pair.charAt(valueStart))) {
                    valueStart++;
                    valueEnd = pair.length() - 1;
                    valueString = pair.substring(valueStart, valueEnd).stripLeading();
                    JsonNode child = ((ObjectNode) root).put(keyString, recurse(valueString, mapper));
                }
            } else {
                valueString = pair.substring(valueStart).stripLeading();
                ObjectNode child = ((ObjectNode)root).put(keyString, valueString);
            }
        }

        return root;
    }

    private static Map<Character, Character> initBracketMap() {
        Map<Character, Character> bracketMap = new HashMap<>();

        bracketMap.put('[', ']');
        bracketMap.put('(', ')');
        bracketMap.put('<', '>');

        return bracketMap;
    }

    private Map<String, Object> createRecursedMap(JsonNode node, ObjectMapper mapper) {
        return mapper.convertValue(node, new TypeReference<HashMap<String, Object>>() {});
    }

    private boolean isIgnoredGroup(String group) {
        // If a group starts and ends with stringLiteralCharacter,
        // treat the entire group as key with null as the value
        return stringLiteralCharacter != null &&
            group.charAt(0) == stringLiteralCharacter &&
            group.charAt(group.length()-1) == stringLiteralCharacter;
    }

    private Map<String, Object> createNonRecursedMap(String[] groups) {
        Map<String, Object> nonRecursedMap = new LinkedHashMap<>();
        List<Object> valueList;

        for(final String group : groups) {
            if (isIgnoredGroup(group)) {
                if (validKeyAndValue(group, null)) {
                    nonRecursedMap.put(group, null);
                }
                continue;
            }

            final String[] terms = keyValueDelimiterPattern.split(group, 2);
            String key = terms[0];
            Object value;

            if (terms.length == 2) {
                value = terms[1];
            } else {
                LOG.debug("Unsuccessful match: '{}'", terms[0]);
                value = keyValueProcessorConfig.getNonMatchValue();
            }

            if (nonRecursedMap.containsKey(key)) {
                Object existingValue = nonRecursedMap.get(key);

                if (existingValue instanceof List) {
                    valueList = (List<Object>) existingValue;
                } else {
                    valueList = new ArrayList<Object>();
                    valueList.add(existingValue);
                    if (validKeyAndValue(key, valueList)) {
                        nonRecursedMap.put(key, valueList);
                    }
                }

                if (keyValueProcessorConfig.getSkipDuplicateValues()) {
                    if (!valueList.contains(value)) {
                        valueList.add(value);
                    }
                } else {
                    valueList.add(value);
                }
            } else {
                if (validKeyAndValue(key, value)) {
                    nonRecursedMap.put(key, value);
                }
            }
        }

        return nonRecursedMap;
    }

    private Map<String, Object> executeConfigs(Map<String, Object> map) {
        Map<String, Object> processed = new HashMap<>();

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

            if (keyValueProcessorConfig.getDeleteKeyRegex() != null && !Objects.equals(keyValueProcessorConfig.getDeleteKeyRegex(), "")) {
                key = key.replaceAll(keyValueProcessorConfig.getDeleteKeyRegex(), "");
            }
            key = keyPrefix + key;

            if (value != null
                    && value instanceof String
                    && keyValueProcessorConfig.getDeleteValueRegex() != null
                    && !Objects.equals(keyValueProcessorConfig.getDeleteValueRegex(), "")) {
                value = ((String)value).replaceAll(keyValueProcessorConfig.getDeleteValueRegex(), "");
            }

            if (keyValueProcessorConfig.getWhitespace() == WhitespaceOption.STRICT) {
                String[] whitespace_arr = trimWhitespace(key, value);
                key = whitespace_arr[0];
                value = whitespace_arr[1];
            }

            if (keyValueProcessorConfig.getTransformKey() != null
                    && keyValueProcessorConfig.getTransformKey() != TransformOption.NONE) {
                key = transformKey(key);
            }

            if (keyValueProcessorConfig.getRemoveBrackets()) {
                final String bracketRegex = "[\\[\\]()<>]";
                if (value != null) {
                    value = value.toString().replaceAll(bracketRegex,"");
                }
            }

            addKeyValueToMap(processed, key, value);
        }

        for (Map.Entry<String,Object> pair : defaultValuesMap.entrySet()) {
            if (processed.containsKey(pair.getKey())) {
                LOG.debug("Skipping already included default key: '{}'", pair.getKey());
                continue;
            }
            if (validKeyAndValue(pair.getKey(), pair.getValue())) {
                String key = pair.getKey();
                processed.put(key, pair.getValue());
            }
        }

        return processed;
    }

    private String[] trimWhitespace(String key, Object value) {
        String[] arr = {key.stripTrailing(), value.toString().stripLeading()};
        return arr;
    }

    private String transformKey(String key) {
        return keyValueProcessorConfig.getTransformKey().getTransformFunction().apply(key);
    }

    private boolean validKeyAndValue(String key, Object value) {
        if (key == null || key.isEmpty()) {
            return false;
        }

        if (keyValueProcessorConfig.getDropKeysWithNoValue() && value == null) {
            return false;
        }
        return true;
    }

    private void addKeyValueToMap(final Map<String, Object> parsedMap, final String key, Object value) {
        Object processedValue = value;

        if (!validKeyAndValue(key, value)) {
            return;
        }

        if (value instanceof List) {
            List<?> valueAsList = (List<?>) value;
            if (valueAsList.size() == 1) {
                processedValue = valueAsList.get(0);
            }
        }

        if (!parsedMap.containsKey(key)) {
            parsedMap.put(key, processedValue);
            return;
        }

        if (parsedMap.get(key) instanceof List) {
            if (keyValueProcessorConfig.getSkipDuplicateValues()) {
                if (((List<Object>) parsedMap.get(key)).contains(processedValue)) {
                    return;
                }
            }

            ((List<Object>) parsedMap.get(key)).add(processedValue);
        } else {
            if (keyValueProcessorConfig.getSkipDuplicateValues()) {
                if (parsedMap.containsValue(processedValue)) {
                    return;
                }
            }

            final LinkedList<Object> combinedList = new LinkedList<>();
            combinedList.add(parsedMap.get(key));
            combinedList.add(processedValue);

            parsedMap.replace(key, combinedList);
        }
    }

    private void writeToRoot(final Event event, final Map<String, Object> parsedJson) {
        for (Map.Entry<String, Object> entry : parsedJson.entrySet()) {
            try {
                if (keyValueProcessorConfig.getOverwriteIfDestinationExists() || !event.containsKey(entry.getKey())) {
                    event.put(entry.getKey(), entry.getValue(), normalizeKeys);
                }
            } catch (IllegalArgumentException e) {
                LOG.warn("Failed to put key: "+entry.getKey()+" value : "+entry.getValue()+" into event. ", e);
            }
        }
    }
}
