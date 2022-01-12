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

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class KeyValueProcessorTests {
    private KeyValueProcessor keyValueProcessor;
    private PluginMetrics pluginMetrics = PluginMetrics.fromNames("kv", "pipeline");

    @BeforeEach
    void setup() {
        keyValueProcessor = new KeyValueProcessor(pluginMetrics, new KeyValueProcessorConfig());
    }

    @Test
    void testSingleKvToObjectKeyValueProcessor() {
        final Map<String, Object> testData = new HashMap();
        testData.put("message", "key1=value1");
        final Record<Event> record = buildRecordWithEvent(testData);

        final List<Record<Event>> editedRecords = (List<Record<Event>>)keyValueProcessor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.size(), equalTo(1));
        assertThat(editedRecords.get(0), notNullValue());
        assertThat(editedRecords.get(0).getData().toJsonString(),
                equalTo("{\"message\":\"key1=value1\",\"parsed_message\":{\"key1\":\"value1\"}}"));
    }

    @Test
    void testMultipleKvToObjectKeyValueProcessor() {
        final Map<String, Object> testData = new HashMap();
        testData.put("message", "key1=value1&key2=value2");
        final Record<Event> record = buildRecordWithEvent(testData);

        final List<Record<Event>> editedRecords = (List<Record<Event>>)keyValueProcessor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.size(), equalTo(1));
        assertThat(editedRecords.get(0), notNullValue());
        assertThat(editedRecords.get(0).getData().toJsonString(),
                equalTo("{\"message\":\"key1=value1&key2=value2\",\"parsed_message\":{\"key1\":\"value1\",\"key2\":\"value2\"}}"));
    }

    @Test
    void testSingleRegexFieldDelimiterKvToObjectKeyValueProcessor() {
        keyValueProcessor = new KeyValueProcessor(pluginMetrics, getConfig("", null, "!_*!", "=", "", ""));

        final Map<String, Object> testData = new HashMap();
        testData.put("message", "key1=value1!_____!key2=value2");
        final Record<Event> record = buildRecordWithEvent(testData);

        final List<Record<Event>> editedRecords = (List<Record<Event>>)keyValueProcessor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.size(), equalTo(1));
        assertThat(editedRecords.get(0), notNullValue());
        assertThat(editedRecords.get(0).getData().toJsonString(),
            equalTo("{\"message\":\"key1=value1!_____!key2=value2\",\"parsed_message\":{\"key1\":\"value1\",\"key2\":\"value2\"}}"));
    }

    @Test
    void testSingleRegexKvDelimiterKvToObjectKeyValueProcessor() {
        //override setup
        keyValueProcessor = new KeyValueProcessor(pluginMetrics, getConfig("", null, "&", ":\\+*:", "", ""));

        final Map<String, Object> testData = new HashMap();
        testData.put("message", "key1:++:value1&key2:+:value2");
        final Record<Event> record = buildRecordWithEvent(testData);

        final List<Record<Event>> editedRecords = (List<Record<Event>>)keyValueProcessor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.size(), equalTo(1));
        assertThat(editedRecords.get(0), notNullValue());
        assertThat(editedRecords.get(0).getData().toJsonString(),
                equalTo("{\"message\":\"key1:++:value1&key2:+:value2\",\"parsed_message\":{\"key1\":\"value1\",\"key2\":\"value2\"}}"));
    }

    @Test
    void testDuplicateKvToArrayValueProcessor() {
        final Map<String, Object> testData = new HashMap();
        testData.put("message", "key1=value1&key1=value2");
        final Record<Event> record = buildRecordWithEvent(testData);

        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.size(), equalTo(1));
        assertThat(editedRecords.get(0), notNullValue());
        assertThat(editedRecords.get(0).getData().toJsonString(),
                equalTo("{\"message\":\"key1=value1&key1=value2\",\"parsed_message\":{\"key1\":[\"value1\",\"value2\"]}}"));
    }

    @Test
    void testCustomPrefixKvProcessor() {
        //override setup
        keyValueProcessor = new KeyValueProcessor(pluginMetrics, getConfig("TEST_", null, "&", ":", "", ""));

        final Map<String, Object> testData = new HashMap();
        testData.put("message", "key1:value1");
        final Record<Event> record = buildRecordWithEvent(testData);

        final List<Record<Event>> editedRecords = (List<Record<Event>>)keyValueProcessor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.size(), equalTo(1));
        assertThat(editedRecords.get(0), notNullValue());
        assertThat(editedRecords.get(0).getData().toJsonString(),
                equalTo("{\"message\":\"key1:value1\",\"parsed_message\":{\"TEST_key1\":\"value1\"}}"));
    }

    @Test
    void testNonMatchValueKvProcessor() {
        //override setup
        keyValueProcessor = new KeyValueProcessor(pluginMetrics, getConfig("", "BAD_MATCH", "&", ":", "", ""));

        final Map<String, Object> testData = new HashMap();
        testData.put("message", "key1+value1");
        final Record<Event> record = buildRecordWithEvent(testData);

        final List<Record<Event>> editedRecords = (List<Record<Event>>)keyValueProcessor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.size(), equalTo(1));
        assertThat(editedRecords.get(0), notNullValue());
        assertThat(editedRecords.get(0).getData().toJsonString(),
                equalTo("{\"message\":\"key1+value1\",\"parsed_message\":{\"key1+value1\":\"BAD_MATCH\"}}"));
    }

    @Test
    void testTrimKeyRegexKvProcessor() {
        //override setup
        keyValueProcessor = new KeyValueProcessor(pluginMetrics, getConfig("", null, "&", "=", "\\s", ""));

        final Map<String, Object> testData = new HashMap();
        testData.put("message", "key1  =value1");
        final Record<Event> record = buildRecordWithEvent(testData);

        final List<Record<Event>> editedRecords = (List<Record<Event>>)keyValueProcessor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.size(), equalTo(1));
        assertThat(editedRecords.get(0), notNullValue());
        assertThat(editedRecords.get(0).getData().toJsonString(),
                equalTo("{\"message\":\"key1  =value1\",\"parsed_message\":{\"key1\":\"value1\"}}"));
    }

    @Test
    void testTrimValueRegexKvProcessor() {
        //override setup
        keyValueProcessor = new KeyValueProcessor(pluginMetrics, getConfig("", null, "&", "=", "", "\\s"));

        final Map<String, Object> testData = new HashMap();
        testData.put("message", "key1=value1   &key2=value2");
        final Record<Event> record = buildRecordWithEvent(testData);

        final List<Record<Event>> editedRecords = (List<Record<Event>>)keyValueProcessor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.size(), equalTo(1));
        assertThat(editedRecords.get(0), notNullValue());
        assertThat(editedRecords.get(0).getData().toJsonString(),
                equalTo("{\"message\":\"key1=value1   &key2=value2\",\"parsed_message\":{\"key1\":\"value1\",\"key2\":\"value2\"}}"));
    }

    @Test
    void testTrimValueAndKeyRegexKvProcessor() {
        //override setup
        keyValueProcessor = new KeyValueProcessor(pluginMetrics, getConfig("", null, "&", "=", "\\s", "\\s"));

        final Map<String, Object> testData = new HashMap();
        testData.put("message", "key1  =value1   & key2 = value2 ");
        final Record<Event> record = buildRecordWithEvent(testData);

        final List<Record<Event>> editedRecords = (List<Record<Event>>)keyValueProcessor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.size(), equalTo(1));
        assertThat(editedRecords.get(0), notNullValue());
        assertThat(editedRecords.get(0).getData().toJsonString(),
                equalTo("{\"message\":\"key1  =value1   & key2 = value2 \",\"parsed_message\":{\"key1\":\"value1\",\"key2\":\"value2\"}}"));
    }

    @Test
    void testShutdownIsReady() {
        assertThat(keyValueProcessor.isReadyForShutdown(), is(true));
    }

    private void reflectivelySetField(final KeyValueProcessorConfig keyValueProcessorConfig, final String fieldName, final Object value) throws NoSuchFieldException, IllegalAccessException {
        final Field field = KeyValueProcessorConfig.class.getDeclaredField(fieldName);
        try {
            field.setAccessible(true);
            field.set(keyValueProcessorConfig, value);
        } finally {
            field.setAccessible(false);
        }
    }

    private KeyValueProcessorConfig getConfig(final String prefix, final String nonMatchValue,
                                              final String fieldDelimiterRegex, final String keyValueDelimiterRegex, final String trimKeyRegex,
                                              final String trimValueRegex) {
        KeyValueProcessorConfig config = new KeyValueProcessorConfig();

        try {
            reflectivelySetField(config, "prefix", prefix);
            reflectivelySetField(config, "nonMatchValue", nonMatchValue);
            reflectivelySetField(config, "fieldDelimiterRegex", fieldDelimiterRegex);
            reflectivelySetField(config, "keyValueDelimiterRegex", keyValueDelimiterRegex);
            reflectivelySetField(config, "trimKeyRegex", trimKeyRegex);
            reflectivelySetField(config, "trimValueRegex", trimValueRegex);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

        return config;
    }

    static Record<Event> buildRecordWithEvent(final Map<String, Object> data) {
        return new Record<>(JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build());
    }
}
