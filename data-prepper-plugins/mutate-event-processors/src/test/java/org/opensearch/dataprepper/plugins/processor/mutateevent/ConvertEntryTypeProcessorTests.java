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
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.record.Record;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ConvertEntryTypeProcessorTests {
    private static final Float MAX_ERROR = 0.00001f;
    static final String TEST_KEY = UUID.randomUUID().toString();
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private ConvertEntryTypeProcessorConfig mockConfig;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    private ConvertEntryTypeProcessor typeConversionProcessor;
    private Random random;

    static Record<Event> buildRecordWithEvent(final Map<String, Object> data) {
        return new Record<>(JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build());
    }

    @BeforeEach
    void setup() {
        random = new Random();
        lenient().when(mockConfig.getKey()).thenReturn(TEST_KEY);
        lenient().when(mockConfig.getKeys()).thenReturn(null);
        lenient().when(mockConfig.getConvertWhen()).thenReturn(null);
    }

    private Record<Event> getMessage(String message, Object value) {
        final Map<String, Object> testData = new HashMap<>();
        testData.put("message", message);
        testData.put(ConvertEntryTypeProcessorTests.TEST_KEY, value);
        return buildRecordWithEvent(testData);
    }

    private Event executeAndGetProcessedEvent(final Object testValue) {
        final Record<Event> record = getMessage(UUID.randomUUID().toString(), testValue);
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
    void invalid_convert_when_throws_InvalidPluginConfigurationException() {
        final String convertWhen = UUID.randomUUID().toString();

        when(mockConfig.getType()).thenReturn(TargetType.fromOptionValue("integer"));
        when(mockConfig.getConvertWhen()).thenReturn(convertWhen);

        when(expressionEvaluator.isValidExpressionStatement(convertWhen)).thenReturn(false);

        assertThrows(InvalidPluginConfigurationException.class, () -> new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator));
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
    void testArrayOfStringsToIntegerConvertEntryTypeProcessor() {
        when(mockConfig.getType()).thenReturn(TargetType.fromOptionValue("integer"));
        typeConversionProcessor = new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator);

        Random random = new Random();
        Integer testValue1 = random.nextInt();
        Integer testValue2 = random.nextInt();
        Integer testValue3 = random.nextInt();
        String[] inputArray = {testValue1.toString(), testValue2.toString(), testValue3.toString()};
        List<Integer> expectedResult = new ArrayList<>();
        expectedResult.add(testValue1);
        expectedResult.add(testValue2);
        expectedResult.add(testValue3);
        Event event = executeAndGetProcessedEvent(inputArray);
        assertThat(event.get(TEST_KEY, List.class), equalTo(expectedResult));
    }

    @Test
    void testArrayListOfStringsToIntegerConvertEntryTypeProcessor() {
        when(mockConfig.getType()).thenReturn(TargetType.fromOptionValue("integer"));
        typeConversionProcessor = new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator);

        Random random = new Random();
        Integer testValue1 = random.nextInt();
        Integer testValue2 = random.nextInt();
        Integer testValue3 = random.nextInt();
        List<String> inputList = new ArrayList<>();
        inputList.add(testValue1.toString());
        inputList.add(testValue2.toString());
        inputList.add(testValue3.toString());
        List<Integer> expectedResult = new ArrayList<>();
        expectedResult.add(testValue1);
        expectedResult.add(testValue2);
        expectedResult.add(testValue3);
        Event event = executeAndGetProcessedEvent(inputList);
        assertThat(event.get(TEST_KEY, List.class), equalTo(expectedResult));
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
        when(mockConfig.getType()).thenReturn(TargetType.fromOptionValue("big_decimal"));
        typeConversionProcessor = new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator);
        Event event = executeAndGetProcessedEvent(testValue.toString());
        assertThat(event.get(TEST_KEY, BigDecimal.class), equalTo(testValue));
    }

    @Test
    void testDecimalToBigDecimalWithScaleConvertEntryTypeProcessor() {
        String testValue = "2147483647";
        TargetType bigdecimalTargetType = TargetType.fromOptionValue("big_decimal");
        when(mockConfig.getType()).thenReturn(bigdecimalTargetType);
        when(mockConfig.getScale()).thenReturn(5);
        typeConversionProcessor = new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator);
        Event event = executeAndGetProcessedEvent(testValue);
        //As we set the scale to 5, we expect to see 5 positions filled with zeros
        assertThat(event.get(TEST_KEY, BigDecimal.class), equalTo(new BigDecimal(testValue+".00000")));
    }

    @ParameterizedTest
    @MethodSource("decimalFormatKeysArgumentProvider")
    void testDecimalToBigDecimalWithRoundingConvertEntryTypeProcessor(String source, String target) {

        TargetType bigdecimalTargetType = TargetType.fromOptionValue("big_decimal");
        when(mockConfig.getType()).thenReturn(bigdecimalTargetType);
        when(mockConfig.getScale()).thenReturn(5);
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
        Double testValue = 123.456;
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
        when(expressionEvaluator.isValidExpressionStatement(convertWhen)).thenReturn(true);

        final Record<Event> record = getMessage(UUID.randomUUID().toString(), testValue);
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
        final Map<String, Object> testData = new HashMap<>();
        testData.put("message", "testMessage");
        testData.put(testKey1, testValue);
        testData.put(testKey2, testValue);
        Record<Event> record = buildRecordWithEvent(testData);
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

    private Long getRandomLong() {
        Long l = random.nextLong();
        if (l < 0 && l >= Integer.MIN_VALUE)
            return Long.MIN_VALUE + l;
        if (l > 0 && l <= Integer.MAX_VALUE)
            return Long.MAX_VALUE - l;
        return l;
    }

    private Double getRandomDouble() {
        Double d = random.nextDouble();
        if (d < 0 && d >= Float.MIN_VALUE)
            return Double.MIN_VALUE + d;
        if (d > 0 && d <= Float.MAX_VALUE)
            return Double.MAX_VALUE - d;
        return d;
    }

    @Test
    void testCoerceStrings() {
        when(mockConfig.getKey()).thenReturn(null);
        when(mockConfig.getKeys()).thenReturn(null);
        when(mockConfig.getCoerceStrings()).thenReturn(new ConvertEntryTypeProcessorConfig.CoerceStringsConfig());
        when(mockConfig.getConvertWhen()).thenReturn(null);
        int i1 = random.nextInt();
        int i2 = random.nextInt();
        int i3 = random.nextInt();
        int i4 = random.nextInt();

        long l1 = getRandomLong();
        long l2 = getRandomLong();
        long l3 = getRandomLong();
        long l4 = getRandomLong();

        float f1 = random.nextFloat();
        float f2 = random.nextFloat();
        float f3 = random.nextFloat();
        float f4 = random.nextFloat();

        double d1 = getRandomDouble();
        double d2 = getRandomDouble();
        double d3 = getRandomDouble();
        double d4 = getRandomDouble();

        String s1 = UUID.randomUUID().toString();
        String s2 = UUID.randomUUID().toString();
        String s3 = UUID.randomUUID().toString();
        String s4 = UUID.randomUUID().toString();

        Instant now = Instant.now();
        ZonedDateTime zonedDateTime = now.atZone(ZoneId.systemDefault());
        ZonedDateTime zonedDateTimePST = now.atZone(ZoneId.of("America/Los_Angeles"));

        String t1 = zonedDateTime.format(DateTimeFormatter.ofPattern(ConvertEntryTypeProcessorConfig.DEFAULT_TIME_STRING_FORMATS.get(0)));
        String t2 = zonedDateTimePST.format(DateTimeFormatter.ofPattern(ConvertEntryTypeProcessorConfig.DEFAULT_TIME_STRING_FORMATS.get(1)));
        String t3 = zonedDateTime.format(DateTimeFormatter.ofPattern(ConvertEntryTypeProcessorConfig.DEFAULT_TIME_STRING_FORMATS.get(2)));
        String t4 = zonedDateTimePST.format(DateTimeFormatter.ofPattern(ConvertEntryTypeProcessorConfig.DEFAULT_TIME_STRING_FORMATS.get(3)));

        final Map<String, Object> testData1 = new HashMap<>();
        testData1.put("s2", s2);
        testData1.put("i2", Integer.toString(i2));
        testData1.put("l2", Long.toString(l2));
        testData1.put("f2", Float.toString(f2));
        testData1.put("d2", Double.toString(d2));
        testData1.put("b2", "false");
        testData1.put("t2", t2);
        final Map<String, Object> testData2 = new HashMap<>();
        testData2.put("s3", s3);
        testData2.put("i3", Integer.toString(i3));
        testData2.put("l3", Long.toString(l3));
        testData2.put("f3", Float.toString(f3));
        testData2.put("d3", Double.toString(d3));
        testData2.put("b3", "true");
        testData2.put("t3", t3);
        final List<Object> list = new ArrayList<>();
        list.add((Object)testData2);
        list.add((Object)Integer.toString(i4));
        list.add((Object)Long.toString(l4));
        list.add((Object)Float.toString(f4));
        list.add((Object)Double.toString(d4));
        list.add("false");
        list.add((Object)t4);

        final Map<String, Object> testData = new HashMap<>();
        testData.put("s1", s1);
        testData.put("i1", Integer.toString(i1));
        testData.put("l1", Long.toString(l1));
        testData.put("f1", Float.toString(f1));
        testData.put("d1", Double.toString(d1));
        testData.put("b1", "true");
        testData.put("t1", t1);
        testData.put("m1", testData1);
        testData.put("a1", list);
        Record<Event> record = buildRecordWithEvent(testData);
        typeConversionProcessor = new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator);
        Event event = executeAndGetProcessedEvent(record);
        assertThat((String)event.get("s1", Object.class), equalTo(s1));
        assertThat((Integer)event.get("i1", Object.class), equalTo(i1));
        assertThat((Long)event.get("l1", Object.class), equalTo(l1));
        assertThat((Double)event.get("f1", Object.class), closeTo(f1, MAX_ERROR));
        assertThat((Double)event.get("d1", Object.class), closeTo(d1, MAX_ERROR));
        assertThat((Boolean)event.get("b1", Object.class), equalTo(true));
        assertThat((Long)event.get("t1", Object.class), equalTo(now.getEpochSecond()*1000));

        assertThat((Integer)event.get("m1/i2", Object.class), equalTo(i2));
        assertThat(event.get("m1/s2", Object.class), equalTo(s2));
        assertThat((Long)event.get("m1/l2", Object.class), equalTo(l2));
        assertThat((Double)event.get("m1/f2", Object.class), closeTo(f2, MAX_ERROR));
        assertThat((Double)event.get("m1/d2", Object.class), closeTo(d2, MAX_ERROR));
        assertThat((Boolean)event.get("m1/b2", Object.class), equalTo(false));
        assertThat((Long)event.get("m1/t2", Object.class), equalTo(now.toEpochMilli()));

        assertThat((Integer)event.get("a1/0/i3", Object.class), equalTo(i3));
        assertThat(event.get("a1/0/s3", Object.class), equalTo(s3));
        assertThat((Long)event.get("a1/0/l3", Object.class), equalTo(l3));
        assertThat((Double)event.get("a1/0/f3", Object.class), closeTo(f3, MAX_ERROR));
        assertThat((Double)event.get("a1/0/d3", Object.class), closeTo(d3, MAX_ERROR));
        assertThat((Boolean)event.get("a1/0/b3", Object.class), equalTo(true));
        assertThat((Long)event.get("a1/0/t3", Object.class), equalTo(now.toEpochMilli()));

        assertThat((Integer)event.get("a1/1", Object.class), equalTo(i4));
        assertThat(event.get("a1/2", Object.class), equalTo(l4));
        assertThat((Double)event.get("a1/3", Object.class), closeTo(f4, MAX_ERROR));
        assertThat((Double)event.get("a1/4", Object.class), closeTo(d4, MAX_ERROR));
        assertThat((Boolean)event.get("a1/5", Object.class), equalTo(false));
        assertThat((Long)event.get("a1/6", Object.class), equalTo(now.getEpochSecond()*1000));
    }

    @Test
    void iterate_on_converts_elements_of_array() {
        final String iterateOn = "list-key";
        final String keyToConvert = "key-to-convert";

        when(mockConfig.getType()).thenReturn(TargetType.fromOptionValue("long"));
        when(mockConfig.getKey()).thenReturn(keyToConvert);
        when(mockConfig.getIterateOn()).thenReturn(iterateOn);

        typeConversionProcessor = new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator);

        final Map<String, Object> eventData = new HashMap<>();

        final String keyDoesNotExist = UUID.randomUUID().toString();
        final String valueForKeyThatDoesNotExist = UUID.randomUUID().toString();
        final String nonConvertableValue = UUID.randomUUID().toString();

        final List<Map<String, Object>> listElement = new ArrayList<>();
        listElement.add(Map.of(keyToConvert, 10.0));
        listElement.add(Map.of(keyToConvert, 20.0));
        listElement.add(Map.of(keyToConvert, nonConvertableValue));
        listElement.add(Map.of(keyDoesNotExist, valueForKeyThatDoesNotExist));

        eventData.put(iterateOn, listElement);

        final Map<String, Object> expectedData = new HashMap<>();

        final List<Map<String, Object>> expectedListElement = new ArrayList<>();
        expectedListElement.add(Map.of(keyToConvert, 10L));
        expectedListElement.add(Map.of(keyToConvert, 20L));
        expectedListElement.add(Map.of(keyToConvert, nonConvertableValue));
        expectedListElement.add(Map.of(keyDoesNotExist, valueForKeyThatDoesNotExist));

        expectedData.put(iterateOn, expectedListElement);

        final Event event = JacksonEvent.builder()
                .withData(eventData)
                .withEventType("event")
                .build();

        final List<Record<Event>> processedRecords = (List<Record<Event>>) typeConversionProcessor.doExecute(Collections.singletonList(new Record<>(event)));
        assertThat(processedRecords.size(), equalTo(1));
        assertThat(processedRecords.get(0).getData().toMap(), equalTo(expectedData));
    }

    @Test
    void iterate_on_with_key_that_is_not_list_of_map_does_not_update() {
        final String iterateOn = "list-key";
        final String keyToConvert = "key-to-convert";

        when(mockConfig.getType()).thenReturn(TargetType.fromOptionValue("long"));
        when(mockConfig.getKey()).thenReturn(keyToConvert);
        when(mockConfig.getIterateOn()).thenReturn(iterateOn);

        typeConversionProcessor = new ConvertEntryTypeProcessor(pluginMetrics, mockConfig, expressionEvaluator);

        final String nonListValue = UUID.randomUUID().toString();
        final Map<String, Object> eventData = new HashMap<>();
        eventData.put(iterateOn, nonListValue);

        final Map<String, Object> expectedData = new HashMap<>();

        expectedData.put(iterateOn, nonListValue);

        final Event event = JacksonEvent.builder()
                .withData(eventData)
                .withEventType("event")
                .build();

        final List<Record<Event>> processedRecords = (List<Record<Event>>) typeConversionProcessor.doExecute(Collections.singletonList(new Record<>(event)));
        assertThat(processedRecords.size(), equalTo(1));
        assertThat(processedRecords.get(0).getData().toMap(), equalTo(expectedData));
    }
}
