package org.opensearch.dataprepper.plugins.processor.translate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.processor.mutateevent.TargetType;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;

@ExtendWith(MockitoExtension.class)
class TranslateProcessorTest {

    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private TranslateProcessorConfig mockConfig;

    @Mock
    private RegexParameterConfiguration mockRegexConfig;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    @BeforeEach
    void setup() {
        lenient().when(mockConfig.getSource()).thenReturn("sourceField");
        lenient().when(mockConfig.getTarget()).thenReturn("targetField");
        lenient().when(mockConfig.getTargetType()).thenReturn(TargetType.STRING);
        lenient().when(mockRegexConfig.getExact()).thenReturn(mockRegexConfig.DEFAULT_EXACT);
    }

    @Test
    void test_string_keys_in_map(){
        when(mockConfig.getMap()).thenReturn(createMapEntries(createMapping("key1","mappedValue1")));
        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("key1");
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", String.class), is("mappedValue1"));
    }

    @Test
    void test_integer_keys_in_map() {
        when(mockConfig.getMap()).thenReturn(createMapEntries(createMapping("123", "mappedValue1")));
        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("123");
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", String.class), is("mappedValue1"));
    }

    @Test
    void test_integer_range_keys_in_map() {
        when(mockConfig.getMap()).thenReturn(createMapEntries(createMapping("1-10", "mappedValue1")));
        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("5");
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", String.class), is("mappedValue1"));

    }

    @Test
    void test_comma_separated_keys_in_map() {
        when(mockConfig.getMap()).thenReturn(createMapEntries(createMapping("key1,key2, key3", "mappedValue1")));
        final TranslateProcessor processor = createObjectUnderTest();

        for (String key : Arrays.asList("key1", "key2", "key3")) {
            final Record<Event> record = getEvent(key);
            final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

            assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
            assertThat(translatedRecords.get(0).getData().get("targetField", String.class), is("mappedValue1"));
        }

        final Record<Event> failureRecord = getEvent("key4");
        final List<Record<Event>> failingTranslatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(failureRecord));

        assertFalse(failingTranslatedRecords.get(0).getData().containsKey("targetField"));
    }

    @Test
    void test_comma_separated_range_keys_in_map() {
        when(mockConfig.getMap()).thenReturn(createMapEntries(createMapping("1-10,11-20, 21-30", "mappedValue1")));
        final TranslateProcessor processor = createObjectUnderTest();

        for (String key : Arrays.asList("5", "15", "25")) {
            final Record<Event> record = getEvent(key);
            final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

            assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
            assertThat(translatedRecords.get(0).getData().get("targetField", String.class), is("mappedValue1"));
        }

        final Record<Event> failureRecord = getEvent("35");
        final List<Record<Event>> failingTranslatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(failureRecord));

        assertFalse(failingTranslatedRecords.get(0).getData().containsKey("targetField"));
    }

    @Test
    void test_float_source() {
        when(mockConfig.getMap()).thenReturn(createMapEntries(createMapping("1-10,11-20, 21-30", "mappedValue1")));
        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("11.1");
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", String.class), is("mappedValue1"));

        final Record<Event> failureRecord = getEvent("20.5");
        final List<Record<Event>> failingTranslatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(failureRecord));

        assertFalse(failingTranslatedRecords.get(0).getData().containsKey("targetField"));
    }

    @Test
    void test_comma_separated_integer_ranges_and_string_keys() {
        when(mockConfig.getMap()).thenReturn(createMapEntries(createMapping("1-10,key1", "mappedValue1")));
        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("5.2");
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", String.class), is("mappedValue1"));

        final Record<Event> recordStringKey = getEvent("key1");
        final List<Record<Event>> translatedStringKeyRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(recordStringKey));

        assertTrue(translatedStringKeyRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedStringKeyRecords.get(0).getData().get("targetField", String.class), is("mappedValue1"));
    }

    @Test
    void test_multiple_dashes_in_keys_should_be_treated_as_string_literal() {
        when(mockConfig.getMap()).thenReturn(createMapEntries(createMapping("1-10-20", "mappedValue1")));
        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> failureRecord = getEvent("1-10-20");
        final List<Record<Event>> failingTranslatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(failureRecord));

        assertTrue(failingTranslatedRecords.get(0).getData().containsKey("targetField"));

        final Record<Event> record = getEvent("10");
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertFalse(translatedRecords.get(0).getData().containsKey("targetField"));

    }

    @Test
    void test_overlapping_ranges_should_fail_when_overlapping() {
        when(mockConfig.getMap()).thenReturn(createMapEntries(createMapping("1-10", "mappedValue1"), createMapping("10-20", "mappedValue2")));

        assertThrows(InvalidPluginConfigurationException.class, () -> createObjectUnderTest());
    }

    @Test
    void test_overlapping_key_and_range_in_map_option() {
        when(mockConfig.getMap()).thenReturn(createMapEntries(createMapping("1-10", "mappedValue1"), createMapping("5.3", "mappedValue2")));

        assertThrows(InvalidPluginConfigurationException.class, () -> createObjectUnderTest());
    }

    @Test
    void test_string_literal_in_pattern_option() {
        when(mockConfig.getRegexParameterConfiguration()).thenReturn(mockRegexConfig);
        when(mockRegexConfig.getPatterns()).thenReturn(createMapEntries(createMapping("key1", "mappedValue1")));

        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("key1");
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", String.class), is("mappedValue1"));

        final Record<Event> failureRecord = getEvent("key2");
        final List<Record<Event>> failingTranslatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(failureRecord));

        assertFalse(failingTranslatedRecords.get(0).getData().containsKey("targetField"));
    }

    @Test
    void test_matching_of_regex_pattern_in_pattern_option() {
        when(mockConfig.getRegexParameterConfiguration()).thenReturn(mockRegexConfig);
        when(mockRegexConfig.getPatterns()).thenReturn(createMapEntries(createMapping("^(1[0-9]|20)$", "patternValue1"))); //Range between 10-20
        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("15");
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", String.class), is("patternValue1"));

        final Record<Event> failureRecord = getEvent("1");
        final List<Record<Event>> failingTranslatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(failureRecord));

        assertFalse(failingTranslatedRecords.get(0).getData().containsKey("targetField"));
    }

    @Test
    void test_pattern_matching_when_no_match_in_map() {
        when(mockConfig.getRegexParameterConfiguration()).thenReturn(mockRegexConfig);
        when(mockConfig.getMap()).thenReturn((createMapEntries(createMapping("key1", "mappedValue1"), createMapping("key2", "mappedValue2"))));
        when(mockRegexConfig.getPatterns()).thenReturn(createMapEntries(createMapping("patternKey1", "patternValue1")));

        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("patternKey1");
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", String.class), is("patternValue1"));

        final Record<Event> recordMapKey = getEvent("key1");
        final List<Record<Event>> translatedMapKeyRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(recordMapKey));

        assertTrue(translatedMapKeyRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedMapKeyRecords.get(0).getData().get("targetField", String.class), is("mappedValue1"));
    }

    @Test
    void test_map_matching_when_overlapping_ranges_in_map_and_pattern() {
        when(mockConfig.getRegexParameterConfiguration()).thenReturn(mockRegexConfig);
        when(mockConfig.getMap()).thenReturn((createMapEntries(createMapping("400", "mappedValue1"))));
        when(mockRegexConfig.getPatterns()).thenReturn(createMapEntries(createMapping("^(400|404)$", "patternValue1"))); // Matches 400 or 404

        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("400");
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", String.class), is("mappedValue1"));

        final Record<Event> recordPatternKey = getEvent("404");
        final List<Record<Event>> translatedPatternKeyRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(recordPatternKey));

        assertTrue(translatedPatternKeyRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedPatternKeyRecords.get(0).getData().get("targetField", String.class), is("patternValue1"));
    }

    @Test
    void test_source_array_single_key() {
        when(mockConfig.getSource()).thenReturn(new ArrayList(List.of("sourceField")));
        when(mockConfig.getMap()).thenReturn((createMapEntries(createMapping("400", "mappedValue1"))));

        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("400");
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", ArrayList.class), is(new ArrayList(List.of("mappedValue1"))));
    }

    @Test
    void test_source_array_multiple_keys() {
        when(mockConfig.getSource()).thenReturn(new ArrayList(List.of("sourceField1", "sourceField2")));
        when(mockConfig.getMap()).thenReturn((createMapEntries(createMapping("key1", "mappedValue1"), createMapping("key2", "mappedValue2"), createMapping("key3", "mappedValue3"))));

        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = buildRecordWithEvent(Map.of("sourceField1", "key1", "sourceField2", "key3"));
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", ArrayList.class), is(new ArrayList(List.of("mappedValue1", "mappedValue3"))));
    }

    @Test
    void test_source_array_with_partial_match_without_default() {
        when(mockConfig.getSource()).thenReturn(new ArrayList(List.of("sourceField1", "sourceField2")));
        when(mockConfig.getMap()).thenReturn((createMapEntries(createMapping("key1", "mappedValue1"), createMapping("key2", "mappedValue2"), createMapping("key3", "mappedValue3"))));

        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = buildRecordWithEvent(Map.of("sourceField1", "key1", "sourceField2", "key4"));
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", ArrayList.class), is(new ArrayList(List.of("mappedValue1"))));
    }

    @Test
    void test_source_array_with_partial_match_with_default() {
        final String defaultValue = "No Match Found";
        when(mockConfig.getSource()).thenReturn(new ArrayList(List.of("sourceField1", "sourceField2")));
        when(mockConfig.getDefaultValue()).thenReturn(defaultValue);
        when(mockConfig.getMap()).thenReturn((createMapEntries(createMapping("key1", "mappedValue1"), createMapping("key2", "mappedValue2"), createMapping("key3", "mappedValue3"))));

        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = buildRecordWithEvent(Map.of("sourceField1", "key1", "sourceField2", "key4"));
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", ArrayList.class), is(new ArrayList(List.of("mappedValue1", defaultValue))));
    }

    @Test
    void test_non_exact_matching() {
        when(mockConfig.getRegexParameterConfiguration()).thenReturn(mockRegexConfig);
        when(mockRegexConfig.getPatterns()).thenReturn(createMapEntries(
                createMapping("^(1[0-9]|20)$", "patternValue1"),
                createMapping("foo", "bar2")));
        when(mockRegexConfig.getExact()).thenReturn(false);

        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("footer");
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", String.class), is("bar2"));

        final Record<Event> regexRecord = getEvent("15");
        final List<Record<Event>> translatedRegexRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(regexRecord));

        assertTrue(translatedRegexRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRegexRecords.get(0).getData().get("targetField", String.class), is("patternValue1"));

        final Record<Event> negativeRecord = getEvent("fo");
        final List<Record<Event>> translatedNegativeRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(negativeRecord));

        assertFalse(translatedNegativeRecords.get(0).getData().containsKey("targetField"));
    }

    @Test
    void test_nested_records_with_default_value() {
        final Map<String, Object> testJson = Map.of("collection", List.of(
                Map.of("sourceField", "key1"),
                Map.of("sourceField", "key2"),
                Map.of("sourceField", "key3")));
        final List<Map<String, Object>> outputJson = List.of(
                Map.of("sourceField", "key1", "targetField", "mappedValue1"),
                Map.of("sourceField", "key2", "targetField", "mappedValue2"),
                Map.of("sourceField", "key3", "targetField", "No Match"));

        when(mockConfig.getRegexParameterConfiguration()).thenReturn(mockRegexConfig);
        when(mockRegexConfig.getPatterns()).thenReturn(createMapEntries(
                createMapping("key1", "mappedValue1"),
                createMapping("key2", "mappedValue2")));
        when(mockConfig.getDefaultValue()).thenReturn("No Match");
        when(mockConfig.getIterateOn()).thenReturn("collection");

        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = buildRecordWithEvent(testJson);
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(translatedRecords.get(0).getData().get("collection", ArrayList.class), is(outputJson));
    }

    @Test
    void test_nested_records_without_default_value() {
        final Map<String, Object> testJson = Map.of("collection", List.of(
                Map.of("sourceField", "key1"),
                Map.of("sourceField", "key2"),
                Map.of("sourceField", "key3")));
        final List<Map<String, Object>> outputJson = List.of(
                Map.of("sourceField", "key1", "targetField", "mappedValue1"),
                Map.of("sourceField", "key2", "targetField", "mappedValue2"),
                Map.of("sourceField", "key3"));

        when(mockConfig.getRegexParameterConfiguration()).thenReturn(mockRegexConfig);
        when(mockRegexConfig.getPatterns()).thenReturn(createMapEntries(
                createMapping("key1", "mappedValue1"),
                createMapping("key2", "mappedValue2")));
        when(mockConfig.getIterateOn()).thenReturn("collection");

        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = buildRecordWithEvent(testJson);
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(translatedRecords.get(0).getData().get("collection", ArrayList.class), is(outputJson));
    }

    @Test
    void test_nested_records_no_match() {
        final Map<String, Object> testJson = Map.of("collection", List.of(
                Map.of("sourceField", "key1"),
                Map.of("sourceField", "key2"),
                Map.of("sourceField", "key3")));
        final List<Map<String, Object>> outputJson = List.of(
                Map.of("sourceField", "key1"),
                Map.of("sourceField", "key2"),
                Map.of("sourceField", "key3"));

        when(mockConfig.getRegexParameterConfiguration()).thenReturn(mockRegexConfig);
        when(mockRegexConfig.getPatterns()).thenReturn(createMapEntries(createMapping("key4", "mappedValue1")));
        when(mockConfig.getIterateOn()).thenReturn("collection");

        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = buildRecordWithEvent(testJson);
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(translatedRecords.get(0).getData().get("collection", ArrayList.class), is(outputJson));
    }

    @Test
    void test_target_type_default(){
        when(mockConfig.getMap()).thenReturn(createMapEntries(createMapping("key1", "200")));
        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("key1");
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", String.class), is("200"));
    }

    @Test
    void test_target_type_integer(){
        when(mockConfig.getMap()).thenReturn(createMapEntries(createMapping("key1", "200")));
        when(mockConfig.getTargetType()).thenReturn(TargetType.INTEGER);
        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("key1");
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", Integer.class), is(200));
    }

    @Test
    void test_target_type_boolean(){
        when(mockConfig.getMap()).thenReturn(createMapEntries(createMapping("key1", "false")));
        when(mockConfig.getTargetType()).thenReturn(TargetType.BOOLEAN);
        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("key1");
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", Boolean.class), is(false));
    }

    @Test
    void test_target_type_double(){
        when(mockConfig.getMap()).thenReturn(createMapEntries(createMapping("key1", "20.3")));
        when(mockConfig.getTargetType()).thenReturn(TargetType.DOUBLE);
        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("key1");
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", Double.class), is(20.3));
    }


    private TranslateProcessor createObjectUnderTest() {
        return new TranslateProcessor(pluginMetrics, mockConfig, expressionEvaluator);
    }

    private Record<Event> sourceAndTargetFields(Object sourceValue, Object targetValue) {
        final Map<String, Object> testData = new HashMap<>();
        testData.put("sourceField", sourceValue);
        testData.put("targetField", targetValue);
        return buildRecordWithEvent(testData);
    }

    private Record<Event> getEvent(Object sourceField) {
        final Map<String, Object> testData = new HashMap<>();
        testData.put("sourceField", sourceField);
        return buildRecordWithEvent(testData);
    }

    private static Record<Event> buildRecordWithEvent(final Map<String, Object> data) {
        return new Record<>(JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build());
    }

    private Map.Entry<String, String> createMapping(String key, String value) {
        return new AbstractMap.SimpleEntry<>(key, value);
    }

    private Map<String, Object> createMapEntries(Map.Entry<String, String>... mappings) {
        final Map<String, Object> finalMap = new HashMap<>();
        for (Map.Entry<String, String> mapping : mappings) {
            finalMap.put(mapping.getKey(), mapping.getValue());
        }

        return finalMap;
    }
}