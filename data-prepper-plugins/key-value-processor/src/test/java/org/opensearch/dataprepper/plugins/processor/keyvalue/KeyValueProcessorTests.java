/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.keyvalue;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.PatternSyntaxException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class KeyValueProcessorTests {
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private KeyValueProcessorConfig mockConfig;

    private KeyValueProcessor keyValueProcessor;

    static Record<Event> buildRecordWithEvent(final Map<String, Object> data) {
        return new Record<>(JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build());
    }

    @BeforeEach
    void setup() {
        final KeyValueProcessorConfig defaultConfig = new KeyValueProcessorConfig();
        lenient().when(mockConfig.getSource()).thenReturn(defaultConfig.getSource());
        lenient().when(mockConfig.getDestination()).thenReturn(defaultConfig.getDestination());
        lenient().when(mockConfig.getFieldDelimiterRegex()).thenReturn(defaultConfig.getFieldDelimiterRegex());
        lenient().when(mockConfig.getFieldSplitCharacters()).thenReturn(defaultConfig.getFieldSplitCharacters());
        lenient().when(mockConfig.getIncludeKeys()).thenReturn(defaultConfig.getIncludeKeys());
        lenient().when(mockConfig.getExcludeKeys()).thenReturn(defaultConfig.getExcludeKeys());
        lenient().when(mockConfig.getDefaultValues()).thenReturn(defaultConfig.getDefaultValues());
        lenient().when(mockConfig.getKeyValueDelimiterRegex()).thenReturn(defaultConfig.getKeyValueDelimiterRegex());
        lenient().when(mockConfig.getValueSplitCharacters()).thenReturn(defaultConfig.getValueSplitCharacters());
        lenient().when(mockConfig.getNonMatchValue()).thenReturn(defaultConfig.getNonMatchValue());
        lenient().when(mockConfig.getPrefix()).thenReturn(defaultConfig.getPrefix());
        lenient().when(mockConfig.getDeleteKeyRegex()).thenReturn(defaultConfig.getDeleteKeyRegex());
        lenient().when(mockConfig.getDeleteValueRegex()).thenReturn(defaultConfig.getDeleteValueRegex());
        lenient().when(mockConfig.getTransformKey()).thenReturn(defaultConfig.getTransformKey());
        lenient().when(mockConfig.getWhitespace()).thenReturn(defaultConfig.getWhitespace());
        lenient().when(mockConfig.getSkipDuplicateValues()).thenReturn(defaultConfig.getSkipDuplicateValues());
        lenient().when(mockConfig.getRemoveBrackets()).thenReturn(defaultConfig.getRemoveBrackets());
        lenient().when(mockConfig.getRecursive()).thenReturn(defaultConfig.getRecursive());
        lenient().when(mockConfig.getOverwriteIfDestinationExists()).thenReturn(defaultConfig.getOverwriteIfDestinationExists());

        keyValueProcessor = new KeyValueProcessor(pluginMetrics, mockConfig);
    }

    @Test
    void testSingleKvToObjectKeyValueProcessor() {
        final Record<Event> record = getMessage("key1=value1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1", "value1");
    }

    @Test
    void testMultipleKvToObjectKeyValueProcessor() {
        final Record<Event> record = getMessage("key1=value1&key2=value2");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "key1", "value1");
        assertThatKeyEquals(parsed_message, "key2", "value2");
    }

    @Test
    void testWriteToRoot() {
        when(mockConfig.getDestination()).thenReturn(null);
        final Record<Event> record = getMessage("key1=value1&key2=value2");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));

        final Event event = editedRecords.get(0).getData();
        assertThat(event.containsKey("parsed_message"), is(false));

        assertThat(event.containsKey("key1"), is(true));
        assertThat(event.containsKey("key2"), is(true));
        assertThat(event.get("key1", Object.class), is("value1"));
        assertThat(event.get("key2", Object.class), is("value2"));
    }

    @Test
    void testWriteToRootWithOverwrite() {
        when(mockConfig.getDestination()).thenReturn(null);
        final Record<Event> record = getMessage("key1=value1&key2=value2");
        record.getData().put("key1", "value to be overwritten");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));

        final Event event = editedRecords.get(0).getData();

        assertThat(event.containsKey("key1"), is(true));
        assertThat(event.containsKey("key2"), is(true));
        assertThat(event.get("key1", Object.class), is("value1"));
        assertThat(event.get("key2", Object.class), is("value2"));
    }

    @Test
    void testWriteToDestinationWithOverwrite() {
        final Record<Event> record = getMessage("key1=value1&key2=value2");
        record.getData().put("parsed_message", "value to be overwritten");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "key1", "value1");
        assertThatKeyEquals(parsed_message, "key2", "value2");
    }

    @Test
    void testWriteToRootWithOverwriteDisabled() {
        when(mockConfig.getDestination()).thenReturn(null);
        when(mockConfig.getOverwriteIfDestinationExists()).thenReturn(false);
        final Record<Event> record = getMessage("key1=value1&key2=value2");
        record.getData().put("key1", "value will not be overwritten");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));

        final Event event = editedRecords.get(0).getData();

        assertThat(event.containsKey("key1"), is(true));
        assertThat(event.containsKey("key2"), is(true));
        assertThat(event.get("key1", Object.class), is("value will not be overwritten"));
        assertThat(event.get("key2", Object.class), is("value2"));
    }

    @Test
    void testWriteToDestinationWithOverwriteDisabled() {
        when(mockConfig.getOverwriteIfDestinationExists()).thenReturn(false);
        final Record<Event> record = getMessage("key1=value1&key2=value2");
        record.getData().put("parsed_message", "value will not be overwritten");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final Event event = editedRecords.get(0).getData();

        assertThat(event.containsKey("parsed_message"), is(true));
        assertThat(event.get("parsed_message", Object.class), is("value will not be overwritten"));
    }

    @Test
    void testSingleRegexFieldDelimiterKvToObjectKeyValueProcessor() {
        when(mockConfig.getFieldDelimiterRegex()).thenReturn(":_*:");
        when(mockConfig.getFieldSplitCharacters()).thenReturn(null);

        keyValueProcessor = new KeyValueProcessor(pluginMetrics, mockConfig);

        final Record<Event> record = getMessage("key1=value1:_____:key2=value2");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        System.out.println(parsed_message);
        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "key1", "value1");
        assertThatKeyEquals(parsed_message, "key2", "value2");
    }

    @Test
    void testBothKeyValuesDefinedErrorKeyValueProcessor() {
        when(mockConfig.getKeyValueDelimiterRegex()).thenReturn(":\\+*:");

        assertThrows(IllegalArgumentException.class, () -> new KeyValueProcessor(pluginMetrics, mockConfig));
    }

    @Test
    void testBothFieldsDefinedErrorKeyValueProcessor() {
        when(mockConfig.getFieldDelimiterRegex()).thenReturn(":\\+*:");

        assertThrows(IllegalArgumentException.class, () -> new KeyValueProcessor(pluginMetrics, mockConfig));
    }

    @Test
    void testSingleRegexKvDelimiterKvToObjectKeyValueProcessor() {
        when(mockConfig.getKeyValueDelimiterRegex()).thenReturn(":\\+*:");
        when(mockConfig.getValueSplitCharacters()).thenReturn(null);

        keyValueProcessor = new KeyValueProcessor(pluginMetrics, mockConfig);

        final Record<Event> record = getMessage("key1:++:value1&key2:+:value2");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "key1", "value1");
        assertThatKeyEquals(parsed_message, "key2", "value2");
    }

    @Test
    void testBadKeyValueDelimiterRegexKeyValueProcessor() {
        when(mockConfig.getKeyValueDelimiterRegex()).thenReturn("[");
        when(mockConfig.getValueSplitCharacters()).thenReturn(null);

        PatternSyntaxException e = assertThrows(PatternSyntaxException.class, () -> new KeyValueProcessor(pluginMetrics, mockConfig));
        assertThat(e.getMessage(), CoreMatchers.startsWith("key_value_delimiter"));
    }

    @Test
    void testBadFieldDelimiterRegexKeyValueProcessor() {
        when(mockConfig.getFieldDelimiterRegex()).thenReturn("[");
        when(mockConfig.getFieldSplitCharacters()).thenReturn(null);

        PatternSyntaxException e = assertThrows(PatternSyntaxException.class, () -> new KeyValueProcessor(pluginMetrics, mockConfig));
        assertThat(e.getMessage(), CoreMatchers.startsWith("field_delimiter"));
    }

    @Test
    void testBadDeleteKeyRegexKeyValueProcessor() {
        when(mockConfig.getDeleteKeyRegex()).thenReturn("[");
        PatternSyntaxException e = assertThrows(PatternSyntaxException.class, () -> new KeyValueProcessor(pluginMetrics, mockConfig));
        assertThat(e.getMessage(), CoreMatchers.startsWith("delete_key_regex"));
    }

    @Test
    void testBadDeleteValueRegexKeyValueProcessor() {
        when(mockConfig.getDeleteValueRegex()).thenReturn("[");
        PatternSyntaxException e = assertThrows(PatternSyntaxException.class, () -> new KeyValueProcessor(pluginMetrics, mockConfig));
        assertThat(e.getMessage(), CoreMatchers.startsWith("delete_value_regex"));
    }

    @Test
    void testDuplicateKeyToArrayValueProcessor() {
        final Record<Event> record = getMessage("key1=value1&key1=value2&key1=value3");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        final ArrayList<Object> expectedValue = new ArrayList<>();
        expectedValue.add("value1");
        expectedValue.add("value2");
        expectedValue.add("value3");
        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1", expectedValue);
    }

    @Test
    void testDuplicateKeyToArrayWithNonMatchValueProcessor() {
        final Record<Event> record = getMessage("key1=value1&key1=value2&key1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        final ArrayList<Object> expectedValue = new ArrayList();
        expectedValue.add("value1");
        expectedValue.add("value2");
        expectedValue.add(null);
        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1", expectedValue);
    }

    @Test
    void testFieldSplitCharactersKeyValueProcessor() {
        when(mockConfig.getFieldSplitCharacters()).thenReturn("&!");
        keyValueProcessor = new KeyValueProcessor(pluginMetrics, mockConfig);

        final Record<Event> record = getMessage("key1=value1&key1=value2!key1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        final ArrayList<Object> expectedValue = new ArrayList();
        expectedValue.add("value1");
        expectedValue.add("value2");
        expectedValue.add(null);
        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1", expectedValue);
    }

    @Test
    void testFieldSplitCharactersDoesntSupercedeDelimiterKeyValueProcessor() {
        when(mockConfig.getFieldDelimiterRegex()).thenReturn(":d+:");
        when(mockConfig.getFieldSplitCharacters()).thenReturn(null);
        keyValueProcessor = new KeyValueProcessor(pluginMetrics, mockConfig);

        final Record<Event> record = getMessage("key1=value1:d:key1=value2:d:key1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        final ArrayList<Object> expectedValue = new ArrayList();
        expectedValue.add("value1");
        expectedValue.add("value2");
        expectedValue.add(null);
        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1", expectedValue);
    }

    @Test
    void testIncludeKeysKeyValueProcessor() {
        final List<String> includeKeys = List.of("key2", "key3");
        when(mockConfig.getIncludeKeys()).thenReturn(includeKeys);
        keyValueProcessor = new KeyValueProcessor(pluginMetrics, mockConfig);

        final Record<Event> record = getMessage("key1=value1&key2=value2&key3=value3");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "key2", "value2");
        assertThatKeyEquals(parsed_message, "key3", "value3");
    }

    @Test
    void testIncludeKeysNoMatchKeyValueProcessor() {
        final List<String> includeKeys = Collections.singletonList("noMatch");
        when(mockConfig.getIncludeKeys()).thenReturn(includeKeys);
        keyValueProcessor = new KeyValueProcessor(pluginMetrics, mockConfig);

        final Record<Event> record = getMessage("key1=value1&key2=value2");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(0));
    }

    @Test
    void testIncludeKeysAsDefaultKeyValueProcessor() {
        when(mockConfig.getIncludeKeys()).thenReturn(List.of());
        keyValueProcessor = new KeyValueProcessor(pluginMetrics, mockConfig);

        final Record<Event> record = getMessage("key1=value1&key2=value2");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "key1", "value1");
        assertThatKeyEquals(parsed_message, "key2", "value2");
    }

    @Test
    void testExcludeKeysKeyValueProcessor() {
        final List<String> excludeKeys = List.of("key2");
        when(mockConfig.getExcludeKeys()).thenReturn(excludeKeys);
        keyValueProcessor = new KeyValueProcessor(pluginMetrics, mockConfig);

        final Record<Event> record = getMessage("key1=value1&key2=value2");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1", "value1");
    }

    @Test
    void testExcludeKeysAsDefaultKeyValueProcessor() {
        when(mockConfig.getExcludeKeys()).thenReturn(List.of());
        keyValueProcessor = new KeyValueProcessor(pluginMetrics, mockConfig);

        final Record<Event> record = getMessage("key1=value1&key2=value2");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "key1", "value1");
        assertThatKeyEquals(parsed_message, "key2", "value2");
    }

    @Test
    void testIncludeExcludeKeysOverlapKeyValueProcessor() {
        final List<String> includeKeys = List.of("key1", "key3");
        final List<String> excludeKeys = List.of("key3");
        when(mockConfig.getIncludeKeys()).thenReturn(includeKeys);
        when(mockConfig.getExcludeKeys()).thenReturn(excludeKeys);

        assertThrows(IllegalArgumentException.class, () -> new KeyValueProcessor(pluginMetrics, mockConfig));
    }

    @Test
    void testDefaultKeysNoOverlapsBetweenEventKvProcessor() {
        final Map<String, Object> defaultMap = Map.of("dKey", "dValue");
        when(mockConfig.getDefaultValues()).thenReturn(defaultMap);
        keyValueProcessor = new KeyValueProcessor(pluginMetrics, mockConfig);

        final Record<Event> record = getMessage("key1=value1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "key1", "value1");
        assertThatKeyEquals(parsed_message, "dKey", "dValue");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testDefaultKeysAlreadyInMessageKvProcessor(boolean skipDuplicateValues) {
        final Map<String, Object> defaultMap = Map.of("dKey", "dValue");
        when(mockConfig.getDefaultValues()).thenReturn(defaultMap);
        when(mockConfig.getSkipDuplicateValues()).thenReturn(skipDuplicateValues);
        keyValueProcessor = new KeyValueProcessor(pluginMetrics, mockConfig);

        final Record<Event> record = getMessage("key1=value1&dKey=abc");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "key1", "value1");
        assertThatKeyEquals(parsed_message, "dKey", "abc");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testDefaultIncludeKeysOverlapKvProcessor(boolean skipDuplicateValues) {
        final Map<String, Object> defaultMap = Map.of("key1", "abc");
        final List<String> includeKeys = List.of("key1");
        when(mockConfig.getDefaultValues()).thenReturn(defaultMap);
        when(mockConfig.getIncludeKeys()).thenReturn(includeKeys);
        when(mockConfig.getSkipDuplicateValues()).thenReturn(skipDuplicateValues);
        keyValueProcessor = new KeyValueProcessor(pluginMetrics, mockConfig);

        final Record<Event> record = getMessage("key1=value1&key2=value2");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1", "value1");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testDefaultPrioritizeIncludeKeysKvProcessor(boolean skipDuplicateValues) {
        final Map<String, Object> defaultMap = Map.of("key2", "value2");
        final List<String> includeKeys = List.of("key1");
        when(mockConfig.getDefaultValues()).thenReturn(defaultMap);
        when(mockConfig.getIncludeKeys()).thenReturn(includeKeys);
        when(mockConfig.getSkipDuplicateValues()).thenReturn(skipDuplicateValues);
        keyValueProcessor = new KeyValueProcessor(pluginMetrics, mockConfig);

        final Record<Event> record = getMessage("key1=value1&key2=abc");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "key1", "value1");
        assertThatKeyEquals(parsed_message, "key2", "value2");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testIncludeKeysNotInRecordMessageKvProcessor(boolean skipDuplicateValues) {
        final Map<String, Object> defaultMap = Map.of("key2", "value2");
        final List<String> includeKeys = List.of("key1");
        when(mockConfig.getDefaultValues()).thenReturn(defaultMap);
        when(mockConfig.getIncludeKeys()).thenReturn(includeKeys);
        when(mockConfig.getSkipDuplicateValues()).thenReturn(skipDuplicateValues);
        keyValueProcessor = new KeyValueProcessor(pluginMetrics, mockConfig);

        final Record<Event> record = getMessage("key2=abc");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key2", "value2");
    }

    @Test
    void testDefaultExcludeKeysOverlapKeyValueProcessor() {
        final Map<String, Object> defaultMap = Map.of("dKey", "dValue");
        final List<String> excludeKeys = List.of("dKey");
        when(mockConfig.getDefaultValues()).thenReturn(defaultMap);
        when(mockConfig.getExcludeKeys()).thenReturn(excludeKeys);

        assertThrows(IllegalArgumentException.class, () -> new KeyValueProcessor(pluginMetrics, mockConfig));
    }

    @Test
    void testCustomPrefixKvProcessor() {
        when(mockConfig.getPrefix()).thenReturn("TEST_");

        final Record<Event> record = getMessage("key1=value1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "TEST_key1", "value1");
    }

    @Test
    void testDefaultNonMatchValueKvProcessor() {
        final Record<Event> record = getMessage("key1+value1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1+value1", null);
    }

    @Test
    void testCustomStringNonMatchValueKvProcessor() {
        when(mockConfig.getNonMatchValue()).thenReturn("BAD_MATCH");

        final Record<Event> record = getMessage("key1+value1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1+value1", "BAD_MATCH");
    }

    @Test
    void testCustomBoolNonMatchValueKvProcessor() {
        when(mockConfig.getNonMatchValue()).thenReturn(true);

        final Record<Event> record = getMessage("key1+value1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1+value1", true);
    }

    @Test
    void testDeleteKeyRegexKvProcessor() {
        when(mockConfig.getDeleteKeyRegex()).thenReturn("\\s");

        final Record<Event> record = getMessage("key1  =value1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1", "value1");
    }

    @Test
    void testDeleteValueRegexKvProcessor() {
        when(mockConfig.getDeleteValueRegex()).thenReturn("\\s");

        final Record<Event> record = getMessage("key1=value1   &key2=value2");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "key1", "value1");
        assertThatKeyEquals(parsed_message, "key2", "value2");
    }

    @Test
    void testDeleteValueWithNonStringRegexKvProcessor() {
        when(mockConfig.getDeleteValueRegex()).thenReturn("\\s");
        when(mockConfig.getNonMatchValue()).thenReturn(3);

        final Record<Event> record = getMessage("key1&key2=value2");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "key1", 3);
        assertThatKeyEquals(parsed_message, "key2", "value2");
    }

    @Test
    void testDeleteValueAndKeyRegexKvProcessor() {
        when(mockConfig.getDeleteKeyRegex()).thenReturn("\\s");
        when(mockConfig.getDeleteValueRegex()).thenReturn("\\s");

        final Record<Event> record = getMessage("key1  =value1  &  key2 = value2 ");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "key1", "value1");
        assertThatKeyEquals(parsed_message, "key2", "value2");
    }

    @Test
    void testLowercaseTransformKvProcessor() {
        when(mockConfig.getTransformKey()).thenReturn("lowercase");

        final Record<Event> record = getMessage("Key1=value1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1", "value1");
    }

    @Test
    void testUppercaseTransformKvProcessor() {
        when(mockConfig.getTransformKey()).thenReturn("uppercase");

        final Record<Event> record = getMessage("key1=value1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "KEY1", "value1");
    }

    @Test
    void testCapitalizeTransformKvProcessor() {
        when(mockConfig.getTransformKey()).thenReturn("capitalize");

        final Record<Event> record = getMessage("key1=value1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsedMessage = getLinkedHashMap(editedRecords);

        assertThat(parsedMessage.size(), equalTo(1));
        assertThatKeyEquals(parsedMessage, "Key1", "value1");
    }

    @Test
    void testStrictWhitespaceKvProcessor() {
        when(mockConfig.getWhitespace()).thenReturn("strict");

        final Record<Event> record = getMessage("key1  =  value1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1", "value1");
    }

    @Test
    void testFalseSkipDuplicateValuesKvProcessor() {
        final Record<Event> record = getMessage("key1=value1&key1=value1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        final ArrayList<Object> expectedValue = new ArrayList();
        expectedValue.add("value1");
        expectedValue.add("value1");

        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1", expectedValue);
    }

    @Test
    void testTrueSkipDuplicateValuesKvProcessor() {
        when(mockConfig.getSkipDuplicateValues()).thenReturn(true);

        final Record<Event> record = getMessage("key1=value1&key1=value1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1", "value1");
    }

    @Test
    void testTrueThreeInputsDuplicateValuesKvProcessor() {
        when(mockConfig.getSkipDuplicateValues()).thenReturn(true);

        final Record<Event> record = getMessage("key1=value1&key1=value2&key1=value1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        final ArrayList<Object> expectedValue = new ArrayList();
        expectedValue.add("value1");
        expectedValue.add("value2");

        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1", expectedValue);
    }

    @Test
    void testTrueRemoveBracketsKvProcessor() {
        when(mockConfig.getRemoveBrackets()).thenReturn(true);

        final Record<Event> record = getMessage("key1=(value1)&key2=[value2]&key3=<value3>");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(3));
        assertThatKeyEquals(parsed_message, "key1", "value1");
        assertThatKeyEquals(parsed_message, "key2", "value2");
        assertThatKeyEquals(parsed_message, "key3", "value3");
    }

    @Test
    void testTrueRemoveMultipleBracketsKvProcessor() {
        when(mockConfig.getRemoveBrackets()).thenReturn(true);

        final Record<Event> record = getMessage("key1=((value1)&key2=[value1][value2]");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "key1", "value1");
        assertThatKeyEquals(parsed_message, "key2", "value1value2");
    }

    @Test
    void testBasicRecursiveKvProcessor() {
        when(mockConfig.getRecursive()).thenReturn(true);

        final Record<Event> record = getMessage("item1=[item1-subitem1=item1-subitem1-value&item1-subitem2=item1-subitem2-value]&item2=item2-value");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        final Map<String, Object> expectedValueMap = new HashMap<>();
        expectedValueMap.put("item1-subitem1", "item1-subitem1-value");
        expectedValueMap.put("item1-subitem2", "item1-subitem2-value");

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "item1", expectedValueMap);
        assertThatKeyEquals(parsed_message, "item2", "item2-value");
    }

    @Test
    void testMultiRecursiveKvProcessor() {
        when(mockConfig.getRecursive()).thenReturn(true);

        final Record<Event> record = getMessage("item1=[item1-subitem1=(inner1=abc&inner2=xyz)&item1-subitem2=item1-subitem2-value]&item2=item2-value");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        final Map<String, Object> expectedValueMap = new HashMap<>();
        final Map<String, Object> nestedInnerMap = new HashMap<>();

        nestedInnerMap.put("inner1", "abc");
        nestedInnerMap.put("inner2", "xyz");
        expectedValueMap.put("item1-subitem1", nestedInnerMap);
        expectedValueMap.put("item1-subitem2", "item1-subitem2-value");

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "item1", expectedValueMap);
        assertThatKeyEquals(parsed_message, "item2", "item2-value");
    }

    @Test
    void testTransformKeyRecursiveKvProcessor() {
        when(mockConfig.getRecursive()).thenReturn(true);
        when(mockConfig.getTransformKey()).thenReturn("capitalize");

        final Record<Event> record = getMessage("item1=[item1-subitem1=item1-subitem1-value&item1-subitem2=item1-subitem2-value]&item2=item2-value");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        final Map<String, Object> expectedValueMap = new HashMap<>();
        expectedValueMap.put("item1-subitem1", "item1-subitem1-value");
        expectedValueMap.put("item1-subitem2", "item1-subitem2-value");

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "Item1", expectedValueMap);
        assertThatKeyEquals(parsed_message, "Item2", "item2-value");
    }

    @Test
    void testIncludeInnerKeyRecursiveKvProcessor() {
        final List<String> includeKeys = List.of("item1-subitem1");
        when(mockConfig.getRecursive()).thenReturn(true);
        when(mockConfig.getIncludeKeys()).thenReturn(includeKeys);
        keyValueProcessor = new KeyValueProcessor(pluginMetrics, mockConfig);

        final Record<Event> record = getMessage("item1=[item1-subitem1=item1-subitem1-value&item1-subitem2=item1-subitem2-value]&item2=item2-value");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(0));
    }

    @Test
    void testExcludeInnerKeyRecursiveKvProcessor() {
        final List<String> excludeKeys = List.of("item1-subitem1");
        when(mockConfig.getRecursive()).thenReturn(true);
        when(mockConfig.getExcludeKeys()).thenReturn(excludeKeys);
        keyValueProcessor = new KeyValueProcessor(pluginMetrics, mockConfig);

        final Record<Event> record = getMessage("item1=[item1-subitem1=item1-subitem1-value&item1-subitem2=item1-subitem2-value]&item2=item2-value");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        final Map<String, Object> expectedValueMap = new HashMap<>();
        expectedValueMap.put("item1-subitem1", "item1-subitem1-value");
        expectedValueMap.put("item1-subitem2", "item1-subitem2-value");

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "item1", expectedValueMap);
        assertThatKeyEquals(parsed_message, "item2", "item2-value");
    }

    @Test
    void testDefaultInnerKeyRecursiveKvProcessor() {
        final Map<String, Object> defaultMap = Map.of("item1-subitem1", "default");
        when(mockConfig.getRecursive()).thenReturn(true);
        when(mockConfig.getDefaultValues()).thenReturn(defaultMap);
        keyValueProcessor = new KeyValueProcessor(pluginMetrics, mockConfig);

        final Record<Event> record = getMessage("item1=[item1-subitem1=item1-subitem1-value&item1-subitem2=item1-subitem2-value]&item2=item2-value");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        final Map<String, Object> expectedValueMap = new HashMap<>();
        expectedValueMap.put("item1-subitem1", "item1-subitem1-value");
        expectedValueMap.put("item1-subitem2", "item1-subitem2-value");

        assertThat(parsed_message.size(), equalTo(3));
        assertThatKeyEquals(parsed_message, "item1", expectedValueMap);
        assertThatKeyEquals(parsed_message, "item2", "item2-value");
        assertThatKeyEquals(parsed_message, "item1-subitem1", "default");
    }

    @Test
    void testTagsAddedWhenParsingFails() {
        when(mockConfig.getRecursive()).thenReturn(true);
        when(mockConfig.getTagsOnFailure()).thenReturn(List.of("tag1", "tag2"));
        keyValueProcessor = new KeyValueProcessor(pluginMetrics, mockConfig);

        final Record<Event> record = getMessage("item1=[]");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap<String, Object> parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(0));
        assertThat(record.getData().getMetadata().hasTags(List.of("tag1", "tag2")), is(true));
    }

    @Test
    void testShutdownIsReady() {
        assertThat(keyValueProcessor.isReadyForShutdown(), is(true));
    }

    private Record<Event> getMessage(String message) {
        final Map<String, Object> testData = new HashMap();
        testData.put("message", message);
        return buildRecordWithEvent(testData);
    }

    private LinkedHashMap<String, Object> getLinkedHashMap(List<Record<Event>> editedRecords) {
        assertThat(editedRecords.size(), equalTo(1));
        assertThat(editedRecords.get(0), notNullValue());
        LinkedHashMap<String, Object> parsed_message = editedRecords.get(0).getData().get("parsed_message", LinkedHashMap.class);
        assertThat(parsed_message, notNullValue());
        return parsed_message;
    }

    private void assertThatKeyEquals(final LinkedHashMap<String, Object> parsed_message, final String key, final Object value) {
        assertThat(parsed_message.containsKey(key), is(true));
        assertThat(parsed_message.get(key), equalTo(value));
    }
}