/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.typeconversion;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.hamcrest.CoreMatchers;
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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TypeConversionProcessorTests {
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private TypeConversionProcessorConfig mockConfig;

    private TypeConversionProcessor typeConversionProcessor;

    static Record<Event> buildRecordWithEvent(final Map<String, Object> data) {
        return new Record<>(JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build());
    }

    private Record<Event> getMessage(String message, String key, Object value) {
        final Map<String, Object> testData = new HashMap();
        testData.put("message", message);
        testData.put(key, value);
        return buildRecordWithEvent(testData);
    }

    private Event executeAndGetProcessedEvent(final Record<Event> record) {
        final List<Record<Event>> processedRecords = (List<Record<Event>>) typeConversionProcessor.doExecute(Collections.singletonList(record));
        assertThat(processedRecords.size(), equalTo(1));
        assertThat(processedRecords.get(0), notNullValue());
        Event event = processedRecords.get(0).getData();
        assertThat(event, notNullValue());
        return event;
    }

    @Test
    void testStringToIntegerTypeConversionProcessor() {
        String testKey = "key1";
        Integer testValue = 123;
        lenient().when(mockConfig.getKey()).thenReturn(testKey);
        lenient().when(mockConfig.getType()).thenReturn("integer");
        typeConversionProcessor = new TypeConversionProcessor(pluginMetrics, mockConfig);
        final Record<Event> record = getMessage("TestMessage1", testKey, testValue.toString());
        Event event = executeAndGetProcessedEvent(record);
        assertThat(event.get(testKey, Integer.class), equalTo(testValue));
    }

    @Test
    void testDoubleToIntegerTypeConversionProcessor() {
        String testKey = "key1";
        Integer testValue = 123;
        Double testDoubleValue = 123.789;
        lenient().when(mockConfig.getKey()).thenReturn(testKey);
        lenient().when(mockConfig.getType()).thenReturn("integer");
        typeConversionProcessor = new TypeConversionProcessor(pluginMetrics, mockConfig);
        final Record<Event> record = getMessage("TestMessage1", testKey, testDoubleValue);
        Event event = executeAndGetProcessedEvent(record);
        assertThat(event.get(testKey, Integer.class), equalTo(testValue));
    }

    @Test
    void testBooleanToIntegerTypeConversionProcessor() {
        String testKey = "key1";
        Integer testValue = 1;
        Boolean testBooleanValue = true;
        lenient().when(mockConfig.getKey()).thenReturn(testKey);
        lenient().when(mockConfig.getType()).thenReturn("integer");
        typeConversionProcessor = new TypeConversionProcessor(pluginMetrics, mockConfig);
        final Record<Event> record = getMessage("TestMessage1", testKey, testBooleanValue);
        Event event = executeAndGetProcessedEvent(record);
        assertThat(event.get(testKey, Integer.class), equalTo(testValue));
    }

    @Test
    void testIntegerTypeConversionProcessorWithInvalidType() {
        String testKey = "key1";
        Map<String, String> testValue = Map.of("key", "value");
        lenient().when(mockConfig.getKey()).thenReturn(testKey);
        lenient().when(mockConfig.getType()).thenReturn("integer");
        typeConversionProcessor = new TypeConversionProcessor(pluginMetrics, mockConfig);
        final Record<Event> record = getMessage("TestMessage1", testKey, testValue.toString());
        assertThrows(IllegalArgumentException.class, () -> executeAndGetProcessedEvent(record));
    }

    @Test
    void testStringToDoubleTypeConversionProcessor() {
        String testKey = "key1";
        Double testValue = 123.123;
        lenient().when(mockConfig.getKey()).thenReturn(testKey);
        lenient().when(mockConfig.getType()).thenReturn("double");
        typeConversionProcessor = new TypeConversionProcessor(pluginMetrics, mockConfig);
        final Record<Event> record = getMessage("TestMessage2", testKey, testValue.toString());
        Event event = executeAndGetProcessedEvent(record);
        assertThat(event.get(testKey, Double.class), equalTo(testValue));
    }

    @Test
    void testLongToDoubleTypeConversionProcessor() {
        String testKey = "key1";
        Long testValue = (long)123;
        lenient().when(mockConfig.getKey()).thenReturn(testKey);
        lenient().when(mockConfig.getType()).thenReturn("double");
        typeConversionProcessor = new TypeConversionProcessor(pluginMetrics, mockConfig);
        final Record<Event> record = getMessage("TestMessage2", testKey, testValue);
        Event event = executeAndGetProcessedEvent(record);
        assertThat(event.get(testKey, Double.class), equalTo((double)testValue));
    }

    @Test
    void testStringToBooleanTypeConversionProcessor() {
        String testKey = "key1";
        Boolean testValue = false;
        lenient().when(mockConfig.getKey()).thenReturn(testKey);
        lenient().when(mockConfig.getType()).thenReturn("boolean");
        typeConversionProcessor = new TypeConversionProcessor(pluginMetrics, mockConfig);
        final Record<Event> record = getMessage("TestMessage3", testKey, testValue.toString());
        Event event = executeAndGetProcessedEvent(record);
        assertThat(event.get(testKey, Boolean.class), equalTo(testValue));
    }

    @Test
    void testIntegerToBooleanTypeConversionProcessor() {
        String testKey = "key1";
        Long testValue = (long)200;
        Boolean expectedValue = true;
        lenient().when(mockConfig.getKey()).thenReturn(testKey);
        lenient().when(mockConfig.getType()).thenReturn("boolean");
        typeConversionProcessor = new TypeConversionProcessor(pluginMetrics, mockConfig);
        final Record<Event> record = getMessage("TestMessage3", testKey, testValue);
        Event event = executeAndGetProcessedEvent(record);
        assertThat(event.get(testKey, Boolean.class), equalTo(expectedValue));
    }

    @Test
    void testIntegerToStringTypeConversionProcessor() {
        String testKey = "key1";
        Integer testValue = 200;
        String expectedValue = testValue.toString();
        lenient().when(mockConfig.getKey()).thenReturn(testKey);
        lenient().when(mockConfig.getType()).thenReturn("string");
        typeConversionProcessor = new TypeConversionProcessor(pluginMetrics, mockConfig);
        final Record<Event> record = getMessage("TestMessage3", testKey, testValue);
        Event event = executeAndGetProcessedEvent(record);
        assertThat(event.get(testKey, String.class), equalTo(expectedValue));
    }

    @Test
    void testDoubleToStringTypeConversionProcessor() {
        String testKey = "key1";
        Double testValue = 123.456;
        String expectedValue = testValue.toString();
        lenient().when(mockConfig.getKey()).thenReturn(testKey);
        lenient().when(mockConfig.getType()).thenReturn("string");
        typeConversionProcessor = new TypeConversionProcessor(pluginMetrics, mockConfig);
        final Record<Event> record = getMessage("TestMessage3", testKey, testValue);
        Event event = executeAndGetProcessedEvent(record);
        assertThat(event.get(testKey, String.class), equalTo(expectedValue));
    }

    @Test
    void testBooleanToStringTypeConversionProcessor() {
        String testKey = "key1";
        Boolean testValue = false;
        String expectedValue = testValue.toString();
        lenient().when(mockConfig.getKey()).thenReturn(testKey);
        lenient().when(mockConfig.getType()).thenReturn("string");
        typeConversionProcessor = new TypeConversionProcessor(pluginMetrics, mockConfig);
        final Record<Event> record = getMessage("TestMessage3", testKey, testValue);
        Event event = executeAndGetProcessedEvent(record);
        assertThat(event.get(testKey, String.class), equalTo(expectedValue));
    }

    @Test
    void testInvalidTypeConversionProcessor() {
        String testKey = "key1";
        Integer testValue = 123;
        lenient().when(mockConfig.getKey()).thenReturn(testKey);
        lenient().when(mockConfig.getType()).thenReturn("float");
        typeConversionProcessor = new TypeConversionProcessor(pluginMetrics, mockConfig);
        final Record<Event> record = getMessage("TestMessage3", testKey, testValue);
        assertThrows(IllegalArgumentException.class, () -> executeAndGetProcessedEvent(record));
    }
}
