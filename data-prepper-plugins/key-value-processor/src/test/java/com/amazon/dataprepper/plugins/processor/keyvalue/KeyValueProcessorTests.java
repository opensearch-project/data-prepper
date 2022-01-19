/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.keyvalue;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.JacksonEvent;
import com.amazon.dataprepper.model.record.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.PatternSyntaxException;

import static org.hamcrest.CoreMatchers.anything;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class KeyValueProcessorTests {
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private KeyValueProcessorConfig mockConfig;

    private KeyValueProcessor keyValueProcessor;

    @BeforeEach
    void setup() {
        final KeyValueProcessorConfig defaultConfig = new KeyValueProcessorConfig();
        lenient().when(mockConfig.getSource()).thenReturn(defaultConfig.getSource());
        lenient().when(mockConfig.getDestination()).thenReturn(defaultConfig.getDestination());
        lenient().when(mockConfig.getFieldDelimiterRegex()).thenReturn(defaultConfig.getFieldDelimiterRegex());
        lenient().when(mockConfig.getKeyValueDelimiterRegex()).thenReturn(defaultConfig.getKeyValueDelimiterRegex());
        lenient().when(mockConfig.getNonMatchValue()).thenReturn(defaultConfig.getNonMatchValue());
        lenient().when(mockConfig.getPrefix()).thenReturn(defaultConfig.getPrefix());
        lenient().when(mockConfig.getDeleteKeyRegex()).thenReturn(defaultConfig.getDeleteKeyRegex());
        lenient().when(mockConfig.getDeleteValueRegex()).thenReturn(defaultConfig.getDeleteValueRegex());

        keyValueProcessor = new KeyValueProcessor(pluginMetrics, mockConfig);
    }

    @Test
    void testSingleKvToObjectKeyValueProcessor() {
        final Record<Event> record = getMessage("key1=value1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>)keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1", "value1");
    }

    @Test
    void testMultipleKvToObjectKeyValueProcessor() {
        final Record<Event> record = getMessage("key1=value1&key2=value2");
        final List<Record<Event>> editedRecords = (List<Record<Event>>)keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "key1", "value1");
        assertThatKeyEquals(parsed_message, "key2", "value2");
    }

    @Test
    void testSingleRegexFieldDelimiterKvToObjectKeyValueProcessor() {
        when(mockConfig.getFieldDelimiterRegex()).thenReturn(":_*:");
        keyValueProcessor = new KeyValueProcessor(pluginMetrics, mockConfig);

        final Record<Event> record = getMessage("key1=value1:_____:key2=value2");
        final List<Record<Event>> editedRecords = (List<Record<Event>>)keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap parsed_message = getLinkedHashMap(editedRecords);

        System.out.println(parsed_message);
        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "key1", "value1");
        assertThatKeyEquals(parsed_message, "key2", "value2");
    }

    @Test
    void testSingleRegexKvDelimiterKvToObjectKeyValueProcessor() {
        when(mockConfig.getKeyValueDelimiterRegex()).thenReturn(":\\+*:");
        keyValueProcessor = new KeyValueProcessor(pluginMetrics, mockConfig);

        final Record<Event> record = getMessage("key1:++:value1&key2:+:value2");
        final List<Record<Event>> editedRecords = (List<Record<Event>>)keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "key1", "value1");
        assertThatKeyEquals(parsed_message, "key2", "value2");
    }

    @Test
    void testBadKeyValueDelimiterRegexKeyValueProcessor() {
        when(mockConfig.getKeyValueDelimiterRegex()).thenReturn("[");
        try {
             keyValueProcessor = new KeyValueProcessor(pluginMetrics, mockConfig);
             assertThat("Exception should have been thrown.", keyValueProcessor, is(not(anything())));
        }
        catch (PatternSyntaxException e) {
            assertThat(e.getMessage().startsWith("key_value_delimiter_regex"), is(true));
        }
        catch (Exception e) {
            assertThat("Should have been PatternSyntaxException", is(true));
        }
    }

    @Test
    void testBadFieldDelimiterRegexKeyValueProcessor() {
        when(mockConfig.getFieldDelimiterRegex()).thenReturn("[");
        try {
            keyValueProcessor = new KeyValueProcessor(pluginMetrics, mockConfig);
            assertThat("Exception should have been thrown.", keyValueProcessor, is(not(anything())));
        }
        catch (PatternSyntaxException e) {
            assertThat(e.getMessage().startsWith("field_delimiter_regex"), is(true));
        }
        catch (Exception e) {
            assertThat("Should have been PatternSyntaxException", is(true));
        }
    }

    @Test
    void testBadDeleteKeyRegexKeyValueProcessor() {
        when(mockConfig.getDeleteKeyRegex()).thenReturn("[");
        try {
            keyValueProcessor = new KeyValueProcessor(pluginMetrics, mockConfig);
            assertThat("Exception should have been thrown.", keyValueProcessor, is(not(anything())));
        }
        catch (PatternSyntaxException e) {
            assertThat(e.getMessage().startsWith("delete_key_regex"), is(true));
        }
        catch (Exception e) {
            assertThat("Should have been PatternSyntaxException", is(true));
        }
    }

    @Test
    void testBadDeleteValueRegexKeyValueProcessor() {
        when(mockConfig.getDeleteValueRegex()).thenReturn("[");
        try {
            keyValueProcessor = new KeyValueProcessor(pluginMetrics, mockConfig);
            assertThat("Exception should have been thrown.", keyValueProcessor, is(not(anything())));
        }
        catch (PatternSyntaxException e) {
            assertThat(e.getMessage().startsWith("delete_value_regex"), is(true));
        }
        catch (Exception e) {
            assertThat("Should have been PatternSyntaxException", is(true));
        }
    }

    @Test
    void testDuplicateKeyToArrayValueProcessor() {
        final Record<Event> record = getMessage("key1=value1&key1=value2&key1=value3");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap parsed_message = getLinkedHashMap(editedRecords);

        final ArrayList expectedValue = new ArrayList();
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
        final LinkedHashMap parsed_message = getLinkedHashMap(editedRecords);

        final ArrayList expectedValue = new ArrayList();
        expectedValue.add("value1");
        expectedValue.add("value2");
        expectedValue.add(null);
        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1", expectedValue);
    }

    @Test
    void testCustomPrefixKvProcessor() {
        when(mockConfig.getPrefix()).thenReturn("TEST_");

        final Record<Event> record = getMessage("key1=value1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>)keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "TEST_key1", "value1");
    }

    @Test
    void testDefaultNonMatchValueKvProcessor() {
        final Record<Event> record = getMessage("key1+value1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>)keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1+value1", null);
    }

    @Test
    void testCustomStringNonMatchValueKvProcessor() {
        when(mockConfig.getNonMatchValue()).thenReturn("BAD_MATCH");

        final Record<Event> record = getMessage("key1+value1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>)keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1+value1", "BAD_MATCH");
    }

    @Test
    void testCustomBoolNonMatchValueKvProcessor() {
        when(mockConfig.getNonMatchValue()).thenReturn(true);

        final Record<Event> record = getMessage("key1+value1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>)keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1+value1", true);
    }

    @Test
    void testDeleteKeyRegexKvProcessor() {
        when(mockConfig.getDeleteKeyRegex()).thenReturn("\\s");

        final Record<Event> record = getMessage("key1  =value1");
        final List<Record<Event>> editedRecords = (List<Record<Event>>)keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(1));
        assertThatKeyEquals(parsed_message, "key1", "value1");
    }

    @Test
    void testDeleteValueRegexKvProcessor() {
        when(mockConfig.getDeleteValueRegex()).thenReturn("\\s");

        final Record<Event> record = getMessage("key1=value1   &key2=value2");
        final List<Record<Event>> editedRecords = (List<Record<Event>>)keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "key1", "value1");
        assertThatKeyEquals(parsed_message, "key2", "value2");
    }

    @Test
    void testDeleteValueWithNonStringRegexKvProcessor() {
        when(mockConfig.getDeleteValueRegex()).thenReturn("\\s");
        when(mockConfig.getNonMatchValue()).thenReturn(3);

        final Record<Event> record = getMessage("key1&key2=value2");
        final List<Record<Event>> editedRecords = (List<Record<Event>>)keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "key1", 3);
        assertThatKeyEquals(parsed_message, "key2", "value2");
    }

    @Test
    void testDeleteValueAndKeyRegexKvProcessor() {
        when(mockConfig.getDeleteKeyRegex()).thenReturn("\\s");
        when(mockConfig.getDeleteValueRegex()).thenReturn("\\s");

        final Record<Event> record = getMessage("key1  =value1   & key2 = value2 ");
        final List<Record<Event>> editedRecords = (List<Record<Event>>)keyValueProcessor.doExecute(Collections.singletonList(record));
        final LinkedHashMap parsed_message = getLinkedHashMap(editedRecords);

        assertThat(parsed_message.size(), equalTo(2));
        assertThatKeyEquals(parsed_message, "key1", "value1");
        assertThatKeyEquals(parsed_message, "key2", "value2");
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

    private LinkedHashMap getLinkedHashMap(List<Record<Event>> editedRecords) {
        assertThat(editedRecords.size(), equalTo(1));
        assertThat(editedRecords.get(0), notNullValue());
        LinkedHashMap parsed_message = editedRecords.get(0).getData().get("parsed_message", LinkedHashMap.class);
        assertThat(parsed_message, notNullValue());
        return parsed_message;
    }

    private void assertThatKeyEquals(final LinkedHashMap parsed_message, final String key, final Object value)
    {
        assertThat(parsed_message.containsKey(key), is(true));
        assertThat(parsed_message.get(key), equalTo(value));
    }

    static Record<Event> buildRecordWithEvent(final Map<String, Object> data) {
        return new Record<>(JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build());
    }
}
