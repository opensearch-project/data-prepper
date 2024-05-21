/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.typeconverter.BigDecimalConverter;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.math.BigDecimal;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ConvertEntryTypeProcessorTests {
    static final String TEST_KEY = UUID.randomUUID().toString();
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private ConvertEntryTypeProcessorConfig mockConfig;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    private ConvertEntryTypeProcessor typeConversionProcessor;

    static Record<Event> buildRecordWithEvent(final Map<String, Object> data) {
        return new Record<>(JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build());
    }

    @BeforeEach
    public void setup() {
        lenient().when(mockConfig.getKey()).thenReturn(TEST_KEY);
        lenient().when(mockConfig.getKeys()).thenReturn(null);
        lenient().when(mockConfig.getConvertWhen()).thenReturn(null);
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
    void testBigDecimalToIntegerConvertEntryTypeProcessor() {
        BigDecimal testValue = new BigDecimal(Integer.MAX_VALUE);
        when(mockConfig.getType()).thenReturn(TargetType.fromOptionValue("integer"));
        typeConversionProcessor = new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator);
        Event event = executeAndGetProcessedEvent(testValue.toString());
        assertThat(event.get(TEST_KEY, Integer.class), equalTo(testValue.intValue()));
    }

    @Test
    void testDecimalToBigDecimalConvertEntryTypeProcessor() {
        BigDecimal testValue = new BigDecimal(Integer.MAX_VALUE);
        when(mockConfig.getType()).thenReturn(TargetType.fromOptionValue("bigdecimal"));
        typeConversionProcessor = new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator);
        Event event = executeAndGetProcessedEvent(testValue.toString());
        assertThat(event.get(TEST_KEY, BigDecimal.class), equalTo(testValue));
    }

    @Test
    void testDecimalToBigDecimalWithScaleConvertEntryTypeProcessor() {
        String testValue = "2147483647";
        TargetType bigdecimalTargetType = TargetType.fromOptionValue("bigdecimal");
        ((BigDecimalConverter)bigdecimalTargetType.getTargetConverter()).setScale(5);
        when(mockConfig.getType()).thenReturn(bigdecimalTargetType);
        typeConversionProcessor = new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator);
        Event event = executeAndGetProcessedEvent(testValue);
        //As we set the scale to 5, we expect to see 5 positions filled with zeros
        assertThat(event.get(TEST_KEY, BigDecimal.class), equalTo(new BigDecimal(testValue+".00000")));
        ((BigDecimalConverter)bigdecimalTargetType.getTargetConverter()).setScale(0);
    }

    @ParameterizedTest
    @MethodSource("decimalFormatKeysArgumentProvider")
    void testDecimalToBigDecimalWithRoundingConvertEntryTypeProcessor(String source, String target) {

        TargetType bigdecimalTargetType = TargetType.fromOptionValue("bigdecimal");
        ((BigDecimalConverter)bigdecimalTargetType.getTargetConverter()).setScale(5);
        when(mockConfig.getType()).thenReturn(bigdecimalTargetType);
        typeConversionProcessor = new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator);
        //Default HALF_ROUND_UP applied for all the conversions
        Event event1 = executeAndGetProcessedEvent(source);
        assertThat(event1.get(TEST_KEY, BigDecimal.class), equalTo(new BigDecimal(target)));
    }

    private static Stream<Arguments> decimalFormatKeysArgumentProvider() {
        //Default HALF_ROUND_UP applied for all the conversions
        return Stream.of(
                Arguments.of("1703908412.707011", "1703908412.70701"),
                Arguments.of("1703908412.707016", "1703908412.70702"),
                Arguments.of("1703908412.707015", "1703908412.70702"),
                Arguments.of("1703908412.707014", "1703908412.70701")
        );
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
    void testMapToStringConvertEntryTypeProcessorWithInvalidTypeWillAddTags() {
        final Map<String, String> testValue = Map.of("key", "value");
        final List<String> tags = List.of("convert_failed");
        when(mockConfig.getType()).thenReturn(TargetType.fromOptionValue("string"));
        when(mockConfig.getTagsOnFailure()).thenReturn(tags);
        typeConversionProcessor = new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator);
        Event event = executeAndGetProcessedEvent(testValue);

        assertThat(event.get(TEST_KEY, Object.class), equalTo(testValue));
        assertThat(event.getMetadata().getTags().size(), equalTo(1));
        assertThat(event.getMetadata().getTags(), containsInAnyOrder(tags.toArray()));
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
    void testBigDecimalToDoubleConvertEntryTypeProcessor() {
        BigDecimal testValue = new BigDecimal(Double.MAX_VALUE);
        when(mockConfig.getType()).thenReturn(TargetType.fromOptionValue("double"));
        typeConversionProcessor = new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator);
        Event event = executeAndGetProcessedEvent(testValue);
        assertThat(event.get(TEST_KEY, Double.class), equalTo(testValue.doubleValue()));
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
    void testBigDecimalToLongConvertEntryTypeProcessor() {
        BigDecimal testValue = new BigDecimal(Long.MAX_VALUE);
        when(mockConfig.getType()).thenReturn(TargetType.fromOptionValue("long"));
        typeConversionProcessor = new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator);
        Event event = executeAndGetProcessedEvent(testValue);
        assertThat(event.get(TEST_KEY, Long.class), equalTo(testValue.longValue()));
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
    void testBigDecimalToStringConvertEntryTypeProcessor() {
        BigDecimal testValue = new BigDecimal(Integer.MAX_VALUE);
        when(mockConfig.getType()).thenReturn(TargetType.fromOptionValue("string"));
        typeConversionProcessor = new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator);
        Event event = executeAndGetProcessedEvent(testValue.toString());
        assertThat(event.get(TEST_KEY, String.class), equalTo(testValue.toString()));
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
    void testDoubleToIntegerConvertEntryTypeProcessorWillAddTags() {
        final Double testDoubleValue = 123.789;
        final List<String> tags = List.of("convert_failed");
        when(mockConfig.getType()).thenReturn(TargetType.fromOptionValue("integer"));
        when(mockConfig.getTagsOnFailure()).thenReturn(tags);
        typeConversionProcessor = new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator);
        Event event = executeAndGetProcessedEvent(testDoubleValue);

        assertThat(event.get(TEST_KEY, Object.class), equalTo(123.789));
        assertThat(event.getMetadata().getTags().size(), equalTo(1));
        assertThat(event.getMetadata().getTags(), containsInAnyOrder(tags.toArray()));
    }

    @Test
    void testNoConversionWhenConvertWhenIsFalse() {
        Integer testValue = 123;
        final String convertWhen = UUID.randomUUID().toString();
        when(mockConfig.getType()).thenReturn(TargetType.fromOptionValue("integer"));
        when(mockConfig.getConvertWhen()).thenReturn(convertWhen);

        final Record<Event> record = getMessage(UUID.randomUUID().toString(), TEST_KEY, testValue);
        when(expressionEvaluator.evaluateConditional(convertWhen, record.getData())).thenReturn(false);
        typeConversionProcessor = new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator);
        Event event = executeAndGetProcessedEvent(record);
        assertThat(event.get(TEST_KEY, Integer.class), equalTo(testValue));
    }

    @Test
    void testMultipleKeysConvertEntryTypeProcessor() {
        Integer testValue = 123;
        String expectedValue = testValue.toString();
        String testKey1 = UUID.randomUUID().toString();
        String testKey2 = UUID.randomUUID().toString();
        when(mockConfig.getKey()).thenReturn(null);
        when(mockConfig.getKeys()).thenReturn(List.of(testKey1, testKey2));
        when(mockConfig.getType()).thenReturn(TargetType.fromOptionValue("string"));
        final Map<String, Object> testData = new HashMap();
        testData.put("message", "testMessage");
        testData.put(testKey1, testValue);
        testData.put(testKey2, testValue);
        Record record = buildRecordWithEvent(testData);
        typeConversionProcessor = new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator);
        Event event = executeAndGetProcessedEvent(record);
        assertThat(event.get(testKey1, String.class), equalTo(expectedValue));
        assertThat(event.get(testKey2, String.class), equalTo(expectedValue));
    }

    @Test
    void testKeyAndKeysBothNullConvertEntryTypeProcessor() {
        when(mockConfig.getKey()).thenReturn(null);
        assertThrows(IllegalArgumentException.class, () -> new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator));
    }

    @Test
    void testKeyAndKeysBothDefinedConvertEntryTypeProcessor() {
        when(mockConfig.getKeys()).thenReturn(Collections.singletonList(TEST_KEY));
        assertThrows(IllegalArgumentException.class, () -> new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator));
    }

    @Test
    void testEmptyKeyConvertEntryTypeProcessor() {
        when(mockConfig.getKey()).thenReturn("");
        assertThrows(IllegalArgumentException.class, () -> new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator));
    }
}
