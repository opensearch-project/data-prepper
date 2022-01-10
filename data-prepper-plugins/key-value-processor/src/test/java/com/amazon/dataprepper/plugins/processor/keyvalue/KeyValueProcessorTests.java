/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.keyvalue;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.JacksonEvent;
import com.amazon.dataprepper.model.record.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
    private PluginSetting pluginSetting;
    private final String PLUGIN_NAME = "kv";

    @BeforeEach
    void setup() {
        pluginSetting = getDefaultPluginSetting(true, "", null, "&", ":");
        pluginSetting.setPipelineName("keyValueProcessorPipeline");
        keyValueProcessor = new KeyValueProcessor(pluginSetting);
    }

    @Test
    void testSingleKvToObjectKeyValueProcessor() {
        final Map<String, Object> testData = new HashMap();
        testData.put("message", "key1:value1");
        final Record<Event> record = buildRecordWithEvent(testData);

        final List<Record<Event>> editedRecords = (List<Record<Event>>)keyValueProcessor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.size(), equalTo(1));
        assertThat(editedRecords.get(0), notNullValue());
        assertThat(editedRecords.get(0).getData().toJsonString(),
                equalTo("{\"message\":\"key1:value1\",\"parsed_message\":{\"key1\":\"value1\"}}"));
    }

    @Test
    void testMultipleKvToObjectKeyValueProcessor() {
        final Map<String, Object> testData = new HashMap();
        testData.put("message", "key1:value1&key2:value2");
        final Record<Event> record = buildRecordWithEvent(testData);

        final List<Record<Event>> editedRecords = (List<Record<Event>>)keyValueProcessor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.size(), equalTo(1));
        assertThat(editedRecords.get(0), notNullValue());
        assertThat(editedRecords.get(0).getData().toJsonString(),
                equalTo("{\"message\":\"key1:value1&key2:value2\",\"parsed_message\":{\"key1\":\"value1\",\"key2\":\"value2\"}}"));
    }

    @Test
    void testSingleRegexFieldDelimiterKvToObjectKeyValueProcessor() {
        //override setup
        pluginSetting = getDefaultPluginSetting(false, "", null, "!_*!", ":");
        pluginSetting.setPipelineName("keyValueProcessorPipeline");
        keyValueProcessor = new KeyValueProcessor(pluginSetting);

        final Map<String, Object> testData = new HashMap();
        testData.put("message", "key1:value1!_____!key2:value2");
        final Record<Event> record = buildRecordWithEvent(testData);

        final List<Record<Event>> editedRecords = (List<Record<Event>>)keyValueProcessor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.size(), equalTo(1));
        assertThat(editedRecords.get(0), notNullValue());
        assertThat(editedRecords.get(0).getData().toJsonString(),
            equalTo("{\"message\":\"key1:value1!_____!key2:value2\",\"parsed_message\":{\"key1\":\"value1\",\"key2\":\"value2\"}}"));
    }

    @Test
    void testSingleRegexKvDelimiterKvToObjectKeyValueProcessor() {
        //override setup
        pluginSetting = getDefaultPluginSetting(false, "", null, "&", ":\\+*:");
        pluginSetting.setPipelineName("keyValueProcessorPipeline");
        keyValueProcessor = new KeyValueProcessor(pluginSetting);

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
        testData.put("message", "key1:value1&key1:value2");
        final Record<Event> record = buildRecordWithEvent(testData);

        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.size(), equalTo(1));
        assertThat(editedRecords.get(0), notNullValue());
        assertThat(editedRecords.get(0).getData().toJsonString(),
                equalTo("{\"message\":\"key1:value1&key1:value2\",\"parsed_message\":{\"key1\":[\"value1\",\"value2\"]}}"));
    }

    @Test
    void testDuplicateSetToFalseKvToValueProcessor() {
        //override setup
        pluginSetting = getDefaultPluginSetting(false, "", null, "&", ":");
        pluginSetting.setPipelineName("keyValueProcessorPipeline");
        keyValueProcessor = new KeyValueProcessor(pluginSetting);

        final Map<String, Object> testData = new HashMap();
        testData.put("message", "key1:value1&key1:value2");
        final Record<Event> record = buildRecordWithEvent(testData);

        final List<Record<Event>> editedRecords = (List<Record<Event>>) keyValueProcessor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.size(), equalTo(1));
        assertThat(editedRecords.get(0), notNullValue());
        assertThat(editedRecords.get(0).getData().toJsonString(),
                equalTo("{\"message\":\"key1:value1&key1:value2\",\"parsed_message\":{\"key1\":\"value2\"}}"));
    }

    @Test
    void testCustomPrefixKvProcessor() {
        //override setup
        pluginSetting = getDefaultPluginSetting(false, "TEST_", null, "&", ":");
        pluginSetting.setPipelineName("keyValueProcessorPipeline");
        keyValueProcessor = new KeyValueProcessor(pluginSetting);

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
        pluginSetting = getDefaultPluginSetting(false, "", "BAD_MATCH", "&", ":");
        pluginSetting.setPipelineName("keyValueProcessorPipeline");
        keyValueProcessor = new KeyValueProcessor(pluginSetting);

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
    void testShutdownIsReady() {
        assertThat(keyValueProcessor.isReadyForShutdown(), is(true));
    }

    private PluginSetting getDefaultPluginSetting(final boolean allow_duplicate_values, final String prefix, final String non_match_value,
        final String field_delimiter_regex, final String key_value_delimiter_regex) {
        final Map<String, Object> settings = new HashMap<>();
        settings.put(KeyValueProcessorConfig.ALLOW_DUPLICATE_VALUES, allow_duplicate_values);
        settings.put(KeyValueProcessorConfig.PREFIX, prefix);
        settings.put(KeyValueProcessorConfig.NON_MATCH_VALUE, non_match_value);
        settings.put(KeyValueProcessorConfig.FIELD_DELIMITER_REGEX, field_delimiter_regex);
        settings.put(KeyValueProcessorConfig.KEY_VALUE_DELIMITER_REGEX, key_value_delimiter_regex);

        return new PluginSetting(PLUGIN_NAME, settings);
    }

    static Record<Event> buildRecordWithEvent(final Map<String, Object> data) {
        return new Record<>(JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build());
    }
}
