/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ConvertEntryTypeProcessorTests {
    static final String TEST_KEY = UUID.randomUUID().toString();
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private ConvertEntryTypeProcessorConfig mockConfig;

    @Mock
    private ExpressionEvaluator<Boolean> expressionEvaluator;

    private ConvertEntryTypeProcessor typeConversionProcessor;

    static Record<Event> buildRecordWithEvent(final Map<String, Object> data) {
        return new Record<>(JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build());
    }

    @BeforeEach
    private void setup() {
        when(mockConfig.getKey()).thenReturn(TEST_KEY);
        when(mockConfig.getConvertWhen()).thenReturn(null);
    }

    private Record<Event> getMessage(String message, String key, Object value) {
        final Map<String, Object> testData = new HashMap();
        testData.put("message", message);
        testData.put(key, value);
        return buildRecordWithEvent(testData);
    }

    private Event executeAndGetProcessedEvent(final Object testValue) {
        final Record<Event> record = getMessage(UUID.randomUUID().toString(), TEST_KEY, testValue);
        final List<Record<Event>> processedRecords = (List<Record<Event>>) typeConversionProcessor.doExecute(Collections.singletonList(record));
        assertThat(processedRecords.size(), equalTo(1));
        assertThat(processedRecords.get(0), notNullValue());
        Event event = processedRecords.get(0).getData();
        assertThat(event, notNullValue());
        return event;
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
    void testStringToIntegerConvertEntryTypeProcessor() {
        Integer testValue = 123;
        when(mockConfig.getType()).thenReturn(TargetType.fromOptionValue("integer"));
        typeConversionProcessor = new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator);
        Event event = executeAndGetProcessedEvent(testValue.toString());
        assertThat(event.get(TEST_KEY, Integer.class), equalTo(testValue));
    }

    @Test
    void testBooleanToIntegerConvertEntryTypeProcessor() {
        int testValue = 1;
        Boolean testBooleanValue = true;
        when(mockConfig.getType()).thenReturn(TargetType.fromOptionValue("integer"));
        typeConversionProcessor = new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator);
        Event event = executeAndGetProcessedEvent(testBooleanValue);
        assertThat(event.get(TEST_KEY, Integer.class), equalTo(testValue));
    }

    @Test
    void testIntegerConvertEntryTypeProcessorWithInvalidType() {
        Map<String, String> testValue = Map.of("key", "value");
        when(mockConfig.getType()).thenReturn(TargetType.fromOptionValue("integer"));
        typeConversionProcessor = new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator);
        assertThrows(IllegalArgumentException.class, () -> executeAndGetProcessedEvent(testValue));
    }

    @Test
    void testStringToDoubleConvertEntryTypeProcessor() {
        Double testValue = 123.123;
        when(mockConfig.getType()).thenReturn(TargetType.fromOptionValue("double"));
        typeConversionProcessor = new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator);
        Event event = executeAndGetProcessedEvent(testValue.toString());
        assertThat(event.get(TEST_KEY, Double.class), equalTo(testValue));
    }

    @Test
    void testLongToDoubleConvertEntryTypeProcessor() {
        Long testValue = (long)123;
        when(mockConfig.getType()).thenReturn(TargetType.fromOptionValue("double"));
        typeConversionProcessor = new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator);
        Event event = executeAndGetProcessedEvent(testValue);
        assertThat(event.get(TEST_KEY, Double.class), equalTo((double)testValue));
    }

    @Test
    void testStringToBooleanConvertEntryTypeProcessor() {
        Boolean testValue = false;
        when(mockConfig.getType()).thenReturn(TargetType.fromOptionValue("boolean"));
        typeConversionProcessor = new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator);
        Event event = executeAndGetProcessedEvent(testValue.toString());
        assertThat(event.get(TEST_KEY, Boolean.class), equalTo(testValue));
    }

    @Test
    void testIntegerToBooleanConvertEntryTypeProcessor() {
        Long testValue = (long)200;
        Boolean expectedValue = true;
        when(mockConfig.getType()).thenReturn(TargetType.fromOptionValue("boolean"));
        typeConversionProcessor = new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator);
        Event event = executeAndGetProcessedEvent(testValue);
        assertThat(event.get(TEST_KEY, Boolean.class), equalTo(expectedValue));
    }

    @Test
    void testIntegerToStringConvertEntryTypeProcessor() {
        Integer testValue = 200;
        String expectedValue = testValue.toString();
        when(mockConfig.getType()).thenReturn(TargetType.fromOptionValue("string"));
        typeConversionProcessor = new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator);
        Event event = executeAndGetProcessedEvent(testValue);
        assertThat(event.get(TEST_KEY, String.class), equalTo(expectedValue));
    }

    @Test
    void testDoubleToStringConvertEntryTypeProcessor() {
        Double testValue = (double)123.456;
        String expectedValue = testValue.toString();
        when(mockConfig.getType()).thenReturn(TargetType.fromOptionValue("string"));
        typeConversionProcessor = new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator);
        Event event = executeAndGetProcessedEvent(testValue);
        assertThat(event.get(TEST_KEY, String.class), equalTo(expectedValue));
    }

    @Test
    void testBooleanToStringConvertEntryTypeProcessor() {
        Boolean testValue = false;
        String expectedValue = testValue.toString();
        when(mockConfig.getType()).thenReturn(TargetType.fromOptionValue("string"));
        typeConversionProcessor = new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator);
        Event event = executeAndGetProcessedEvent(testValue);
        assertThat(event.get(TEST_KEY, String.class), equalTo(expectedValue));
    }

    @Test
    void testInvalidConvertEntryTypeProcessor() {
        Double testDoubleValue = (double)123.789;
        when(mockConfig.getType()).thenReturn(TargetType.fromOptionValue("integer"));
        typeConversionProcessor = new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator);
        assertThrows(IllegalArgumentException.class, () -> executeAndGetProcessedEvent(testDoubleValue));
    }

    @Test
    void testNoConversionWhenConvertWhenIsFalse() {
        Integer testValue = 123;
        final String convertWhen = UUID.randomUUID().toString();
        when(mockConfig.getType()).thenReturn(TargetType.fromOptionValue("integer"));
        when(mockConfig.getConvertWhen()).thenReturn(convertWhen);

        final Record<Event> record = getMessage(UUID.randomUUID().toString(), TEST_KEY, testValue);
        when(expressionEvaluator.evaluate(convertWhen, record.getData())).thenReturn(false);
        typeConversionProcessor = new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator);
        Event event = executeAndGetProcessedEvent(record);
        assertThat(event.get(TEST_KEY, Integer.class), equalTo(testValue));
    }
}
