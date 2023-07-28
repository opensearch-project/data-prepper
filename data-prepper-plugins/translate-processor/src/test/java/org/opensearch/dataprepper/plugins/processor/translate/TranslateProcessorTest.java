package org.opensearch.dataprepper.plugins.processor.translate;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;

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

    @Mock
    private TargetsParameterConfig targetsParameterConfig;

    @Mock
    private MappingsParameterConfig mappingsParameterConfig;

    @BeforeEach
    void setup() {
        lenient()
                .when(mappingsParameterConfig.getSource())
                .thenReturn("sourceField");
        lenient()
                .when(targetsParameterConfig.getTargetType())
                .thenReturn(TargetType.STRING);
        lenient()
                .when(mockRegexConfig.getExact())
                .thenReturn(mockRegexConfig.DEFAULT_EXACT);
        lenient()
                .when(mockConfig.getMappingsParameterConfigs())
                .thenReturn(List.of(mappingsParameterConfig));
        lenient()
                .when(mockConfig.getCombinedMappingsConfigs())
                .thenReturn(List.of(mappingsParameterConfig));
    }

    @Test
    void test_string_keys_in_map() {
        targetsParameterConfig = new TargetsParameterConfig(createMapEntries(createMapping("key1", "mappedValue1")),
                                                            "targetField", null, null, null, null);
        when(mappingsParameterConfig.getTargetsParameterConfigs()).thenReturn(List.of(targetsParameterConfig));
        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("key1");
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", String.class), is("mappedValue1"));
    }

    @Test
    void test_integer_keys_in_map() {
        targetsParameterConfig = new TargetsParameterConfig(createMapEntries(createMapping("123", "mappedValue1")),
                                                            "targetField", null, null, null, null);
        when(mappingsParameterConfig.getTargetsParameterConfigs()).thenReturn(List.of(targetsParameterConfig));
        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("123");
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", String.class), is("mappedValue1"));
    }

    @Test
    void test_integer_range_keys_in_map() {
        targetsParameterConfig = new TargetsParameterConfig(createMapEntries(createMapping("1-10", "mappedValue1")),
                                                            "targetField", null, null, null, null);
        when(mappingsParameterConfig.getTargetsParameterConfigs()).thenReturn(List.of(targetsParameterConfig));
        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("5");
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(
                Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", String.class), is("mappedValue1"));

    }

    @Test
    void test_comma_separated_keys_in_map() {
        targetsParameterConfig = new TargetsParameterConfig(
                createMapEntries(createMapping("key1,key2, key3", "mappedValue1")), "targetField", null, null, null,
                null);
        when(mappingsParameterConfig.getTargetsParameterConfigs()).thenReturn(List.of(targetsParameterConfig));
        final TranslateProcessor processor = createObjectUnderTest();

        for (String key : Arrays.asList("key1", "key2", "key3")) {
            final Record<Event> record = getEvent(key);
            final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(
                    Collections.singletonList(record));

            assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
            assertThat(translatedRecords.get(0).getData().get("targetField", String.class), is("mappedValue1"));
        }

        final Record<Event> failureRecord = getEvent("key4");
        final List<Record<Event>> failingTranslatedRecords = (List<Record<Event>>) processor.doExecute(
                Collections.singletonList(failureRecord));

        assertFalse(failingTranslatedRecords.get(0).getData().containsKey("targetField"));
    }

    @Test
    void test_comma_separated_range_keys_in_map() {
        targetsParameterConfig = new TargetsParameterConfig(
                createMapEntries(createMapping("1-10,11-20, 21-30", "mappedValue1")), "targetField", null, null, null,
                null);
        when(mappingsParameterConfig.getTargetsParameterConfigs()).thenReturn(List.of(targetsParameterConfig));
        final TranslateProcessor processor = createObjectUnderTest();

        for (String key : Arrays.asList("5", "15", "25")) {
            final Record<Event> record = getEvent(key);
            final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(
                    Collections.singletonList(record));

            assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
            assertThat(translatedRecords.get(0).getData().get("targetField", String.class), is("mappedValue1"));
        }

        final Record<Event> failureRecord = getEvent("35");
        final List<Record<Event>> failingTranslatedRecords = (List<Record<Event>>) processor.doExecute(
                Collections.singletonList(failureRecord));

        assertFalse(failingTranslatedRecords.get(0).getData().containsKey("targetField"));
    }

    @Test
    void test_float_source() {
        targetsParameterConfig = new TargetsParameterConfig(
                createMapEntries(createMapping("1-10,11-20, 21-30", "mappedValue1")), "targetField", null, null, null,
                null);
        when(mappingsParameterConfig.getTargetsParameterConfigs()).thenReturn(List.of(targetsParameterConfig));
        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("11.1");
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(
                Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", String.class), is("mappedValue1"));

        final Record<Event> failureRecord = getEvent("20.5");
        final List<Record<Event>> failingTranslatedRecords = (List<Record<Event>>) processor.doExecute(
                Collections.singletonList(failureRecord));

        assertFalse(failingTranslatedRecords.get(0).getData().containsKey("targetField"));
    }

    @Test
    void test_comma_separated_integer_ranges_and_string_keys() {
        targetsParameterConfig = new TargetsParameterConfig(
                createMapEntries(createMapping("1-10,key1", "mappedValue1")), "targetField", null, null, null, null);
        when(mappingsParameterConfig.getTargetsParameterConfigs()).thenReturn(List.of(targetsParameterConfig));
        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("5.2");
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(
                Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", String.class), is("mappedValue1"));

        final Record<Event> recordStringKey = getEvent("key1");
        final List<Record<Event>> translatedStringKeyRecords = (List<Record<Event>>) processor.doExecute(
                Collections.singletonList(recordStringKey));

        assertTrue(translatedStringKeyRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedStringKeyRecords.get(0).getData().get("targetField", String.class), is("mappedValue1"));
    }

    @Test
    void test_multiple_dashes_in_keys_should_be_treated_as_string_literal() {
        targetsParameterConfig = new TargetsParameterConfig(createMapEntries(createMapping("1-10-20", "mappedValue1")),
                                                            "targetField", null, null, null, null);
        when(mappingsParameterConfig.getTargetsParameterConfigs()).thenReturn(List.of(targetsParameterConfig));
        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> failureRecord = getEvent("1-10-20");
        final List<Record<Event>> failingTranslatedRecords = (List<Record<Event>>) processor.doExecute(
                Collections.singletonList(failureRecord));

        assertTrue(failingTranslatedRecords.get(0).getData().containsKey("targetField"));

        final Record<Event> record = getEvent("10");
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(
                Collections.singletonList(record));

        assertFalse(translatedRecords.get(0).getData().containsKey("targetField"));

    }

    @Test
    void test_overlapping_ranges_should_fail_when_overlapping() {
        assertThrows(InvalidPluginConfigurationException.class, () -> new TargetsParameterConfig(
                createMapEntries(createMapping("1-10", "mappedValue1"), createMapping("10-20", "mappedValue2")),
                "targetField", null, null, null, null));
    }

    @Test
    void test_overlapping_key_and_range_in_map_option() {
        assertThrows(InvalidPluginConfigurationException.class, () -> new TargetsParameterConfig(
                createMapEntries(createMapping("1-10", "mappedValue1"), createMapping("5.3", "mappedValue2")),
                "targetField", null, null, null, null));
    }

    @Test
    void test_string_literal_in_pattern_option() {
        when(mockRegexConfig.getPatterns()).thenReturn(createMapEntries(createMapping("key1", "mappedValue1")));
        targetsParameterConfig = new TargetsParameterConfig(null, "targetField", mockRegexConfig, null, null, null);
        when(mappingsParameterConfig.getTargetsParameterConfigs()).thenReturn(List.of(targetsParameterConfig));

        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("key1");
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(
                Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", String.class), is("mappedValue1"));

        final Record<Event> failureRecord = getEvent("key2");
        final List<Record<Event>> failingTranslatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(failureRecord));

        assertFalse(failingTranslatedRecords.get(0).getData().containsKey("targetField"));
    }

    @Test
    void test_matching_of_regex_pattern_in_pattern_option() {
        when(mockRegexConfig.getPatterns()).thenReturn(
                createMapEntries(createMapping("^(1[0-9]|20)$", "patternValue1")));
        targetsParameterConfig = new TargetsParameterConfig(null, "targetField", mockRegexConfig, null, null, null);
        when(mappingsParameterConfig.getTargetsParameterConfigs()).thenReturn(List.of(targetsParameterConfig));
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
        when(mockRegexConfig.getPatterns()).thenReturn(createMapEntries(createMapping("patternKey1", "patternValue1")));
        targetsParameterConfig = new TargetsParameterConfig(
                (createMapEntries(createMapping("key1", "mappedValue1"), createMapping("key2", "mappedValue2"))),
                "targetField", mockRegexConfig, null, null, null);
        when(mappingsParameterConfig.getTargetsParameterConfigs()).thenReturn(List.of(targetsParameterConfig));

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
        when(mockRegexConfig.getPatterns()).thenReturn(createMapEntries(createMapping("^(400|404)$", "patternValue1")));
        targetsParameterConfig = new TargetsParameterConfig(
                (createMapEntries(createMapping("400", "mappedValue1"), createMapping("key2", "mappedValue2"))),
                "targetField", mockRegexConfig, null, null, null);
        when(mappingsParameterConfig.getTargetsParameterConfigs()).thenReturn(List.of(targetsParameterConfig));

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
        when(mappingsParameterConfig.getSource()).thenReturn(new ArrayList(List.of("sourceField")));
        targetsParameterConfig = new TargetsParameterConfig(createMapEntries(createMapping("400", "mappedValue1")),
                                                            "targetField", null, null, null, null);
        when(mappingsParameterConfig.getTargetsParameterConfigs()).thenReturn(List.of(targetsParameterConfig));

        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("400");
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", ArrayList.class), is(new ArrayList(List.of("mappedValue1"))));
    }

    @Test
    void test_source_array_multiple_keys() {
        when(mappingsParameterConfig.getSource()).thenReturn(new ArrayList(List.of("sourceField1", "sourceField2")));
        targetsParameterConfig = new TargetsParameterConfig(
                createMapEntries(createMapping("key1", "mappedValue1"), createMapping("key2", "mappedValue2"),
                                 createMapping("key3", "mappedValue3")), "targetField", null, null, null, null);
        when(mappingsParameterConfig.getTargetsParameterConfigs()).thenReturn(List.of(targetsParameterConfig));

        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = buildRecordWithEvent(Map.of("sourceField1", "key1", "sourceField2", "key3"));
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", ArrayList.class), is(new ArrayList(List.of("mappedValue1", "mappedValue3"))));
    }

    @Test
    void test_source_array_with_partial_match_without_default() {
        when(mappingsParameterConfig.getSource()).thenReturn(new ArrayList(List.of("sourceField1", "sourceField2")));
        targetsParameterConfig = new TargetsParameterConfig(
                createMapEntries(createMapping("key1", "mappedValue1"), createMapping("key2", "mappedValue2"),
                                 createMapping("key3", "mappedValue3")), "targetField", null, null, null, null);
        when(mappingsParameterConfig.getTargetsParameterConfigs()).thenReturn(List.of(targetsParameterConfig));

        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = buildRecordWithEvent(Map.of("sourceField1", "key1", "sourceField2", "key4"));
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", ArrayList.class), is(new ArrayList(List.of("mappedValue1"))));
    }

    @Test
    void test_source_array_with_partial_match_with_default() {
        final String defaultValue = "No Match Found";
        when(mappingsParameterConfig.getSource()).thenReturn(new ArrayList(List.of("sourceField1", "sourceField2")));
        targetsParameterConfig = new TargetsParameterConfig(
                createMapEntries(createMapping("key1", "mappedValue1"), createMapping("key2", "mappedValue2"),
                                 createMapping("key3", "mappedValue3")), "targetField", null, null, defaultValue, null);
        when(mappingsParameterConfig.getTargetsParameterConfigs()).thenReturn(List.of(targetsParameterConfig));

        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = buildRecordWithEvent(Map.of("sourceField1", "key1", "sourceField2", "key4"));
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", ArrayList.class), is(new ArrayList(List.of("mappedValue1", defaultValue))));
    }

    @Test
    void test_non_exact_matching() {
        when(mockRegexConfig.getExact()).thenReturn(false);
        when(mockRegexConfig.getPatterns()).thenReturn(createMapEntries(
                createMapping("^(1[0-9]|20)$", "patternValue1"),
                createMapping("foo", "bar")));
        targetsParameterConfig = new TargetsParameterConfig(null, "targetField", mockRegexConfig, null, null, null);
        when(mappingsParameterConfig.getTargetsParameterConfigs()).thenReturn(List.of(targetsParameterConfig));

        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("footer");
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", String.class), is("barter"));

        final Record<Event> replaceAllRecord = getEvent("foofoo");
        final List<Record<Event>> translatedReplaceAllRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(replaceAllRecord));

        assertTrue(translatedReplaceAllRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedReplaceAllRecords.get(0).getData().get("targetField", String.class), is("barbar"));

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

        when(mockRegexConfig.getPatterns()).thenReturn(createMapEntries(
                createMapping("key1", "mappedValue1"),
                createMapping("key2", "mappedValue2")));
        when(mappingsParameterConfig.getSource()).thenReturn("collection/sourceField");
        targetsParameterConfig = new TargetsParameterConfig(null, "targetField", mockRegexConfig, null, "No Match",
                                                            null);
        when(mappingsParameterConfig.getTargetsParameterConfigs()).thenReturn(List.of(targetsParameterConfig));

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

        when(mockRegexConfig.getPatterns()).thenReturn(createMapEntries(
                createMapping("key1", "mappedValue1"),
                createMapping("key2", "mappedValue2")));
        when(mappingsParameterConfig.getSource()).thenReturn("collection/sourceField");
        targetsParameterConfig = new TargetsParameterConfig(null, "targetField", mockRegexConfig, null, null, null);
        when(mappingsParameterConfig.getTargetsParameterConfigs()).thenReturn(List.of(targetsParameterConfig));

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

        when(mockRegexConfig.getPatterns()).thenReturn(createMapEntries(createMapping("key4", "mappedValue1")));
        when(mappingsParameterConfig.getSource()).thenReturn("collection/sourceField");
        targetsParameterConfig = new TargetsParameterConfig(null, "targetField", mockRegexConfig, null, null, null);
        when(mappingsParameterConfig.getTargetsParameterConfigs()).thenReturn(List.of(targetsParameterConfig));

        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = buildRecordWithEvent(testJson);
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(translatedRecords.get(0).getData().get("collection", ArrayList.class), is(outputJson));
    }

    @Test
    void test_nested_multiple_levels() {
        final Map<String, Object> testJson = Map.of("collection", List.of(
                Map.of("sourceField1", List.of(Map.of("sourceField2", "key1")))));
        final List<Map<String, Object>> outputJson = List.of(
                Map.of("sourceField1", List.of(Map.of("sourceField2", "key1", "targetField","mappedValue1"))));

        when(mockRegexConfig.getPatterns()).thenReturn(createMapEntries(createMapping("key1", "mappedValue1")));
        when(mappingsParameterConfig.getSource()).thenReturn("collection/sourceField1/sourceField2");
        targetsParameterConfig = new TargetsParameterConfig(null, "targetField", mockRegexConfig, null, null, null);
        when(mappingsParameterConfig.getTargetsParameterConfigs()).thenReturn(List.of(targetsParameterConfig));

        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = buildRecordWithEvent(testJson);
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(translatedRecords.get(0).getData().get("collection", ArrayList.class), is(outputJson));
    }

    @Test
    void test_no_path_found_with_wrong_field() {
        final Map<String, Object> testJson = Map.of("collection", List.of(
                Map.of("sourceField1", List.of(Map.of("sourceField2", "key1")))));
        final List<Map<String, Object>> outputJson = List.of(
                Map.of("sourceField1", List.of(Map.of("sourceField2", "key1"))));

        when(mockRegexConfig.getPatterns()).thenReturn(createMapEntries(createMapping("key1", "mappedValue1")));
        when(mappingsParameterConfig.getSource()).thenReturn("collection/noSource/sourceField2");
        targetsParameterConfig = new TargetsParameterConfig(null, "targetField", mockRegexConfig, null, null, null);
        when(mappingsParameterConfig.getTargetsParameterConfigs()).thenReturn(List.of(targetsParameterConfig));

        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = buildRecordWithEvent(testJson);
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(translatedRecords.get(0).getData().get("collection", ArrayList.class), is(outputJson));
    }

    @Test
    void test_no_path_found_with_no_list() {
        final Map<String, Object> testJson = Map.of("collection", List.of(
                Map.of("sourceField1", "key1","sourceField2", "key1")));
        final List<Map<String, Object>> outputJson = List.of(
                Map.of("sourceField1", "key1","sourceField2", "key1"));

        when(mockRegexConfig.getPatterns()).thenReturn(createMapEntries(createMapping("key1", "mappedValue1")));
        when(mappingsParameterConfig.getSource()).thenReturn("collection/sourceField1/sourceField2");
        targetsParameterConfig = new TargetsParameterConfig(null, "targetField", mockRegexConfig, null, null, null);
        when(mappingsParameterConfig.getTargetsParameterConfigs()).thenReturn(List.of(targetsParameterConfig));

        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = buildRecordWithEvent(testJson);
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(translatedRecords.get(0).getData().get("collection", ArrayList.class), is(outputJson));
    }

    @Test
    void test_path_with_whitespaces() {
        final Map<String, Object> testJson = Map.of("collection", List.of(
                Map.of("sourceField1", List.of(Map.of("sourceField2", "key1")))));
        final List<Map<String, Object>> outputJson = List.of(
                Map.of("sourceField1", List.of(Map.of("sourceField2", "key1", "targetField","mappedValue1"))));

        when(mockRegexConfig.getPatterns()).thenReturn(createMapEntries(createMapping("key1", "mappedValue1")));
        when(mappingsParameterConfig.getSource()).thenReturn(" collection/sourceField1/sourceField2  ");
        targetsParameterConfig = new TargetsParameterConfig(null, "targetField", mockRegexConfig, null, null, null);
        when(mappingsParameterConfig.getTargetsParameterConfigs()).thenReturn(List.of(targetsParameterConfig));

        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = buildRecordWithEvent(testJson);
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(translatedRecords.get(0).getData().get("collection", ArrayList.class), is(outputJson));
    }

    @Test
    void test_target_type_default() {
        targetsParameterConfig = new TargetsParameterConfig(createMapEntries(createMapping("key1", "200")),
                                                            "targetField", null, null, null, null);
        when(mappingsParameterConfig.getTargetsParameterConfigs()).thenReturn(List.of(targetsParameterConfig));
        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("key1");
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", String.class), is("200"));
    }

    @Test
    void test_target_type_integer() {
        targetsParameterConfig = new TargetsParameterConfig(createMapEntries(createMapping("key1", "200")),
                                                            "targetField", null, null, null, TargetType.INTEGER);
        when(mappingsParameterConfig.getTargetsParameterConfigs()).thenReturn(List.of(targetsParameterConfig));
        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("key1");
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", Integer.class), is(200));
    }

    @Test
    void test_target_type_boolean() {
        targetsParameterConfig = new TargetsParameterConfig(createMapEntries(createMapping("key1", "false")),
                                                            "targetField", null, null, null, TargetType.BOOLEAN);
        when(mappingsParameterConfig.getTargetsParameterConfigs()).thenReturn(List.of(targetsParameterConfig));
        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("key1");
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", Boolean.class), is(false));
    }

    @Test
    void test_target_type_double() {
        targetsParameterConfig = new TargetsParameterConfig(createMapEntries(createMapping("key1", "20.3")),
                                                            "targetField", null, null, null, TargetType.DOUBLE);
        when(mappingsParameterConfig.getTargetsParameterConfigs()).thenReturn(List.of(targetsParameterConfig));
        final TranslateProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("key1");
        final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", Double.class), is(20.3));
    }

    @Nested
    class FilePathTests {
        private File testMappingsFile;
        private String filePath;
        TranslateProcessorConfig fileTranslateConfig;
        FileParameterConfig fileParameterConfig;

        @BeforeEach
        void setup() throws IOException, NoSuchFieldException, IllegalAccessException {
            testMappingsFile = File.createTempFile("test", ".yaml");
            String fileContent = "mappings:\n" +
                                 "  - source: sourceField\n" +
                                 "    targets:\n" +
                                 "      - target: fileTarget\n" +
                                 "        map:\n" +
                                 "          key1: fileMappedValue";
            Files.write(testMappingsFile.toPath(), fileContent.getBytes());
            filePath = testMappingsFile.getAbsolutePath();
            fileTranslateConfig = new TranslateProcessorConfig();
            fileParameterConfig = new FileParameterConfig();
            setField(FileParameterConfig.class, fileParameterConfig, "fileName", filePath);
            setField(TranslateProcessorConfig.class, fileTranslateConfig, "fileParameterConfig", fileParameterConfig);
        }

        @AfterEach
        void cleanup() {
            testMappingsFile.delete();
        }

        @Test
        void test_only_file_path() throws NoSuchFieldException, IllegalAccessException {
            parseMappings();

            final TranslateProcessor processor = createObjectUnderTest();
            final Record<Event> record = getEvent("key1");
            final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

            assertTrue(translatedRecords.get(0).getData().containsKey("fileTarget"));
            assertThat(translatedRecords.get(0).getData().get("fileTarget", String.class), is("fileMappedValue"));
        }
        @Test
        void test_non_overlapping_sources() throws NoSuchFieldException, IllegalAccessException {
            targetsParameterConfig = new TargetsParameterConfig(createMapEntries(createMapping("key2", "mappedValue2")),
                                                                "targetField", null, null, null, null);
            MappingsParameterConfig fileMappingConfig = createMappingConfig();
            setField(TranslateProcessorConfig.class, fileTranslateConfig, "mappingsParameterConfigs", List.of(fileMappingConfig));
            parseMappings();

            final TranslateProcessor processor = createObjectUnderTest();
            final Record<Event> record = getEvent("key1");
            final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

            assertTrue(translatedRecords.get(0).getData().containsKey("fileTarget"));
            assertThat(translatedRecords.get(0).getData().get("fileTarget", String.class), is("fileMappedValue"));

            final Record<Event> mappingsRecord = getEvent("key2");
            final List<Record<Event>> translatedMappingsRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(mappingsRecord));

            assertTrue(translatedMappingsRecords.get(0).getData().containsKey("targetField"));
            assertThat(translatedMappingsRecords.get(0).getData().get("targetField", String.class), is("mappedValue2"));
        }
        @Test
        void test_overlapping_sources_different_targets() throws NoSuchFieldException, IllegalAccessException {
            targetsParameterConfig = new TargetsParameterConfig(createMapEntries(createMapping("key1", "mappedValue1")),
                                                                "targetField", null, null, null, null);
            MappingsParameterConfig fileMappingConfig = createMappingConfig();
            setField(TranslateProcessorConfig.class, fileTranslateConfig, "mappingsParameterConfigs", List.of(fileMappingConfig));
            parseMappings();

            final TranslateProcessor processor = createObjectUnderTest();
            final Record<Event> record = getEvent("key1");
            final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

            assertTrue(translatedRecords.get(0).getData().containsKey("fileTarget"));
            assertThat(translatedRecords.get(0).getData().get("fileTarget", String.class), is("fileMappedValue"));

            final Record<Event> mappingsRecord = getEvent("key1");
            final List<Record<Event>> translatedMappingsRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(mappingsRecord));

            assertTrue(translatedMappingsRecords.get(0).getData().containsKey("targetField"));
            assertThat(translatedMappingsRecords.get(0).getData().get("targetField", String.class), is("mappedValue1"));
        }

        @Test
        void test_overlapping_sources_and_overlapping_targets() throws NoSuchFieldException, IllegalAccessException {
            targetsParameterConfig = new TargetsParameterConfig(createMapEntries(createMapping("key1", "mappedValue1")),
                                                                "fileTarget", null, null, null, null);
            MappingsParameterConfig fileMappingConfig = createMappingConfig();
            setField(TranslateProcessorConfig.class, fileTranslateConfig, "mappingsParameterConfigs", List.of(fileMappingConfig));
            parseMappings();

            final TranslateProcessor processor = createObjectUnderTest();
            final Record<Event> record = getEvent("key1");
            final List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

            assertTrue(translatedRecords.get(0).getData().containsKey("fileTarget"));
            assertThat(translatedRecords.get(0).getData().get("fileTarget", String.class), is("mappedValue1"));
        }

        void parseMappings(){
            fileTranslateConfig.hasMappings();
            fileTranslateConfig.getCombinedMappingsConfigs().get(0).parseMappings();
            when(mockConfig.getCombinedMappingsConfigs()).thenReturn(fileTranslateConfig.getCombinedMappingsConfigs());
        }
        MappingsParameterConfig createMappingConfig() throws NoSuchFieldException, IllegalAccessException {
            MappingsParameterConfig fileMappingConfig = new MappingsParameterConfig();
            setField(MappingsParameterConfig.class, fileMappingConfig, "source", "sourceField");
            setField(MappingsParameterConfig.class, fileMappingConfig, "targetsParameterConfigs", List.of(targetsParameterConfig));
            return fileMappingConfig;
        }
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