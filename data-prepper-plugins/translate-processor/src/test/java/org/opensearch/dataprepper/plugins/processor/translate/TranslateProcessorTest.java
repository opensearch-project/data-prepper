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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.AbstractMap;
import java.util.Arrays;

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
    void setup(){
        lenient().when(mockConfig.getSource()).thenReturn("sourceField");
        lenient().when(mockConfig.getTarget()).thenReturn("targetField");
    }

    @Test
    public void test_string_keys_in_map(){
        when(mockConfig.getMap()).thenReturn(createMapEntries(createMapping("key1","mappedValue1")));
        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("key1");
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", String.class), is("mappedValue1"));
    }

    @Test
    public void test_integer_keys_in_map(){
        when(mockConfig.getMap()).thenReturn(createMapEntries(createMapping("123","mappedValue1")));
        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("123");
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", String.class), is("mappedValue1"));
    }

    @Test
    public void test_integer_range_keys_in_map(){
        when(mockConfig.getMap()).thenReturn(createMapEntries(createMapping("1-10","mappedValue1")));
        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("5");
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", String.class), is("mappedValue1"));

    }

    @Test
    public void test_comma_separated_keys_in_map(){
        when(mockConfig.getMap()).thenReturn(createMapEntries(createMapping("key1,key2, key3","mappedValue1")));
        final TranslateProcessor processor = createObjectUnderTest();

        for(String key : Arrays.asList("key1","key2","key3")){
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
    public void test_comma_separated_range_keys_in_map(){
        when(mockConfig.getMap()).thenReturn(createMapEntries(createMapping("1-10,11-20, 21-30","mappedValue1")));
        final TranslateProcessor processor = createObjectUnderTest();

        for(String key : Arrays.asList("5","15","25")){
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
    public void test_float_source(){
        when(mockConfig.getMap()).thenReturn(createMapEntries(createMapping("1-10,11-20, 21-30","mappedValue1")));
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
    public void test_comma_separated_integer_ranges_and_string_keys(){
        when(mockConfig.getMap()).thenReturn(createMapEntries(createMapping("1-10,key1","mappedValue1")));
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
    public void test_multiple_dashes_in_keys_should_be_treated_as_string_literal(){
        when(mockConfig.getMap()).thenReturn(createMapEntries(createMapping("1-10-20","mappedValue1")));
        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> failureRecord = getEvent("1-10-20");
        final List<Record<Event>> failingTranslatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(failureRecord));

        assertTrue(failingTranslatedRecords.get(0).getData().containsKey("targetField"));

        final Record<Event> record = getEvent("10");
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertFalse(translatedRecords.get(0).getData().containsKey("targetField"));

    }

    @Test
    public void test_overlapping_ranges_should_fail_when_overlapping(){
        when(mockConfig.getMap()).thenReturn(createMapEntries(createMapping("1-10","mappedValue1"), createMapping("10-20", "mappedValue2")));

        assertThrows(InvalidPluginConfigurationException.class,() -> createObjectUnderTest());
    }

    @Test
    public void test_overlapping_key_and_range_in_map_option(){
        when(mockConfig.getMap()).thenReturn(createMapEntries(createMapping("1-10","mappedValue1"), createMapping("5.3", "mappedValue2")));

        assertThrows(InvalidPluginConfigurationException.class,() -> createObjectUnderTest());
    }

    @Test
    public void test_string_literal_in_pattern_option(){
        when(mockConfig.getRegexParameterConfiguration()).thenReturn(mockRegexConfig);
        when(mockRegexConfig.getPatterns()).thenReturn(createMapEntries(createMapping("key1","mappedValue1")));

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
    public void test_matching_of_regex_pattern_in_pattern_option(){
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
    public void test_pattern_matching_when_no_match_in_map(){
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
    public void test_map_matching_when_overlapping_ranges_in_map_and_pattern(){
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

    public Map.Entry<String, String> createMapping(String key, String value){
        return new AbstractMap.SimpleEntry<>(key, value);
    }

    public Map<String, String> createMapEntries(Map.Entry<String, String>... mappings){
        final Map<String, String> finalMap = new HashMap<>();
        for(Map.Entry<String, String> mapping : mappings){
            finalMap.put(mapping.getKey(), mapping.getValue());
        }

        return finalMap;
    }
}