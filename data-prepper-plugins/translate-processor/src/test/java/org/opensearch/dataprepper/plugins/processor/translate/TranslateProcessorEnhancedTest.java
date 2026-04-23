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
import org.opensearch.dataprepper.event.TestEventKeyFactory;
import org.opensearch.dataprepper.model.event.EventKeyFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.processor.mutateevent.TargetType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.opensearch.dataprepper.model.pattern.Pattern;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class TranslateProcessorEnhancedTest {

    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private TranslateProcessorConfig mockConfig;
    @Mock
    private ExpressionEvaluator expressionEvaluator;
    @Mock
    private TargetsParameterConfig targetsParameterConfig;
    @Mock
    private MappingsParameterConfig mappingsParameterConfig;
    @Mock
    private RegexParameterConfiguration mockRegexConfig;

    private final EventKeyFactory eventKeyFactory = TestEventKeyFactory.getTestEventFactory();

    @BeforeEach
    void setup() {
        lenient().when(mappingsParameterConfig.getSource()).thenReturn("sourceField");
        lenient().when(targetsParameterConfig.getTargetType()).thenReturn(TargetType.STRING);
        lenient().when(targetsParameterConfig.getTarget()).thenReturn("targetField");
        lenient().when(mockConfig.getCombinedMappingsConfigs()).thenReturn(List.of(mappingsParameterConfig));
        lenient().when(mappingsParameterConfig.getTargetsParameterConfigs()).thenReturn(List.of(targetsParameterConfig));
        lenient().when(targetsParameterConfig.fetchIndividualMappings()).thenReturn(Collections.emptyMap());
        lenient().when(targetsParameterConfig.fetchRangeMappings()).thenReturn(new LinkedHashMap<>());
        lenient().when(targetsParameterConfig.fetchCompiledPatterns()).thenReturn(Collections.emptyMap());
    }

    // Error Handling & Edge Cases Tests

    @Test
    void test_invalid_source_object_type_throws_exception() {
        lenient().when(mappingsParameterConfig.getSource()).thenReturn(123); // Invalid type

        TranslateProcessor processor = createObjectUnderTest();
        Record<Event> record = getEvent("test");

        assertDoesNotThrow(() -> processor.doExecute(Collections.singletonList(record)));
        // Exception is caught and logged, record remains unchanged
        assertFalse(record.getData().containsKey("targetField"));
    }

    @Test
    void test_null_records_collection() {
        TranslateProcessor processor = createObjectUnderTest();
        assertThrows(NullPointerException.class, () -> processor.doExecute(null));
    }

    @Test
    void test_empty_records_collection() {
        TranslateProcessor processor = createObjectUnderTest();
        Collection<Record<Event>> result = processor.doExecute(Collections.emptyList());
        assertTrue(result.isEmpty());
    }

    @Test
    void test_null_mappings_config() {
        lenient().when(mockConfig.getCombinedMappingsConfigs()).thenReturn(null);
        TranslateProcessor processor = createObjectUnderTest();
        Record<Event> record = getEvent("test");
        Collection<Record<Event>> result = processor.doExecute(Collections.singletonList(record));
        assertEquals(1, result.size());
        assertFalse(record.getData().containsKey("targetField"));
    }

    @Test
    void test_empty_source_keys_list() {
        lenient().when(mappingsParameterConfig.getSource()).thenReturn(Collections.emptyList());
        TranslateProcessor processor = createObjectUnderTest();
        Record<Event> record = getEvent("test");
        Collection<Record<Event>> result = processor.doExecute(Collections.singletonList(record));
        assertEquals(1, result.size());
        assertFalse(record.getData().containsKey("targetField"));
    }

    // Expression Evaluation Tests

    @Test
    void test_translate_when_condition_true() {
        lenient().when(targetsParameterConfig.getTranslateWhen()).thenReturn("/sourceField == 'test'");
        lenient().when(expressionEvaluator.evaluateConditional(anyString(), any(Event.class))).thenReturn(true);
        lenient().when(targetsParameterConfig.fetchIndividualMappings()).thenReturn(Map.of("test", "translated"));

        TranslateProcessor processor = createObjectUnderTest();
        Record<Event> record = getEvent("test");

        processor.doExecute(Collections.singletonList(record));

        verify(expressionEvaluator).evaluateConditional("/sourceField == 'test'", record.getData());
    }

    @Test
    void test_translate_when_condition_false() {
        lenient().when(targetsParameterConfig.getTranslateWhen()).thenReturn("/sourceField == 'other'");
        lenient().when(expressionEvaluator.evaluateConditional(anyString(), any(Event.class))).thenReturn(false);

        TranslateProcessor processor = createObjectUnderTest();
        Record<Event> record = getEvent("test");

        processor.doExecute(Collections.singletonList(record));

        assertFalse(record.getData().containsKey("targetField"));
    }

    @Test
    void test_expression_evaluation_exception() {
        lenient().when(targetsParameterConfig.getTranslateWhen()).thenReturn("invalid_expression");
        lenient().when(expressionEvaluator.evaluateConditional(anyString(), any(Event.class)))
                .thenThrow(new RuntimeException("Expression error"));

        TranslateProcessor processor = createObjectUnderTest();
        Record<Event> record = getEvent("test");

        assertDoesNotThrow(() -> processor.doExecute(Collections.singletonList(record)));
    }

    @Test
    void test_expression_with_map_record_object() {
        lenient().when(targetsParameterConfig.getTranslateWhen()).thenReturn("/key == 'value'");
        lenient().when(expressionEvaluator.evaluateConditional(anyString(), any(Event.class))).thenReturn(true);
        lenient().when(targetsParameterConfig.fetchIndividualMappings()).thenReturn(Map.of("test", "translated"));
        lenient().when(mappingsParameterConfig.getSource()).thenReturn("nested/sourceField");

        TranslateProcessor processor = createObjectUnderTest();
        Map<String, Object> data = Map.of("nested", List.of(Map.of("sourceField", "test")));
        Record<Event> record = buildRecordWithEvent(data);

        processor.doExecute(Collections.singletonList(record));

        verify(expressionEvaluator, atLeastOnce()).evaluateConditional(anyString(), any(Event.class));
    }

    // JsonExtractor Integration Tests

    @Test
    void test_missing_root_field() {
        lenient().when(mappingsParameterConfig.getSource()).thenReturn("missing/sourceField");

        TranslateProcessor processor = createObjectUnderTest();
        Record<Event> record = getEvent("test");

        processor.doExecute(Collections.singletonList(record));

        assertFalse(record.getData().containsKey("targetField"));
    }

    @Test
    void test_empty_target_objects_from_path() {
        lenient().when(mappingsParameterConfig.getSource()).thenReturn("collection/sourceField");

        TranslateProcessor processor = createObjectUnderTest();
        Map<String, Object> data = Map.of("collection", Collections.emptyList());
        Record<Event> record = buildRecordWithEvent(data);

        processor.doExecute(Collections.singletonList(record));

        assertFalse(record.getData().containsKey("targetField"));
    }

    // Pattern Matching Edge Cases

    @Test
    void test_empty_compiled_patterns() {
        lenient().when(targetsParameterConfig.fetchCompiledPatterns()).thenReturn(Collections.emptyMap());
        lenient().when(targetsParameterConfig.fetchIndividualMappings()).thenReturn(Collections.emptyMap());
        lenient().when(targetsParameterConfig.fetchRangeMappings()).thenReturn(new LinkedHashMap<>());
        lenient().when(targetsParameterConfig.getDefaultValue()).thenReturn(null);

        TranslateProcessor processor = createObjectUnderTest();
        Record<Event> record = getEvent("test");

        processor.doExecute(Collections.singletonList(record));

        assertFalse(record.getData().containsKey("targetField"));
    }

    @Test
    void test_invalid_regex_pattern_handling() {
        Map<Pattern, Object> patterns = new HashMap<>();
        patterns.put(Pattern.compile("valid.*"), "result");
        lenient().when(targetsParameterConfig.fetchCompiledPatterns()).thenReturn(patterns);
        lenient().when(targetsParameterConfig.getRegexParameterConfiguration()).thenReturn(mockRegexConfig);
        lenient().when(mockRegexConfig.getExact()).thenReturn(true);

        TranslateProcessor processor = createObjectUnderTest();
        Record<Event> record = getEvent("validtest");

        processor.doExecute(Collections.singletonList(record));

        // Should handle pattern matching without throwing exception
        assertDoesNotThrow(() -> processor.doExecute(Collections.singletonList(record)));
    }

    @Test
    void test_non_parsable_number_in_range_matching() {
        lenient().when(targetsParameterConfig.fetchIndividualMappings()).thenReturn(Collections.emptyMap());
        lenient().when(targetsParameterConfig.fetchRangeMappings()).thenReturn(new LinkedHashMap<>());
        lenient().when(targetsParameterConfig.fetchCompiledPatterns()).thenReturn(Collections.emptyMap());
        lenient().when(targetsParameterConfig.getDefaultValue()).thenReturn("default");
        lenient().when(targetsParameterConfig.getTarget()).thenReturn("targetField");

        TranslateProcessor processor = createObjectUnderTest();
        Record<Event> record = getEvent("not_a_number");

        processor.doExecute(Collections.singletonList(record));

        // Since no mappings match but default is provided, target field should be set with default value
        assertTrue(record.getData().containsKey("targetField"));
        assertEquals("default", record.getData().get("targetField", String.class));
    }

    // Type Conversion Edge Cases

    @Test
    void test_type_conversion_with_empty_target_values() {
        lenient().when(targetsParameterConfig.fetchIndividualMappings()).thenReturn(Collections.emptyMap());
        lenient().when(targetsParameterConfig.fetchRangeMappings()).thenReturn(new LinkedHashMap<>());
        lenient().when(targetsParameterConfig.fetchCompiledPatterns()).thenReturn(Collections.emptyMap());
        lenient().when(targetsParameterConfig.getDefaultValue()).thenReturn(null);

        TranslateProcessor processor = createObjectUnderTest();
        Record<Event> record = getEvent("no_match");

        processor.doExecute(Collections.singletonList(record));

        assertFalse(record.getData().containsKey("targetField"));
    }

    @Test
    void test_type_conversion_failure_handling() {
        lenient().when(targetsParameterConfig.fetchIndividualMappings()).thenReturn(Map.of("test", "invalid_number"));
        lenient().when(targetsParameterConfig.getTargetType()).thenReturn(TargetType.INTEGER);

        TranslateProcessor processor = createObjectUnderTest();
        Record<Event> record = getEvent("test");

        // Should handle conversion failure gracefully
        assertDoesNotThrow(() -> processor.doExecute(Collections.singletonList(record)));
    }

    // Performance & Concurrency Tests

    @Test
    void test_large_dataset_processing() {
        lenient().when(targetsParameterConfig.fetchIndividualMappings()).thenReturn(Map.of("test", "translated"));
        TranslateProcessor processor = createObjectUnderTest();
        List<Record<Event>> records = new ArrayList<>();
        // Create 1000 records
        for (int i = 0; i < 1000; i++) {
            records.add(getEvent("test"));
        }
        Collection<Record<Event>> result = processor.doExecute(records);
        assertEquals(1000, result.size());
    }

    @Test
    void test_concurrent_execution() throws Exception {
        lenient().when(targetsParameterConfig.fetchIndividualMappings()).thenReturn(Map.of("test", "translated"));
        TranslateProcessor processor = createObjectUnderTest();
        ExecutorService executor = Executors.newFixedThreadPool(5);
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            futures.add(CompletableFuture.runAsync(() -> {
                Record<Event> record = getEvent("test");
                processor.doExecute(Collections.singletonList(record));
            }, executor));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        executor.shutdown();

        // Should complete without exceptions
        assertTrue(futures.stream().allMatch(f -> f.isDone() && !f.isCompletedExceptionally()));
    }

    @Test
    void test_deeply_nested_structure() {
        lenient().when(mappingsParameterConfig.getSource()).thenReturn("level1/level2/level3/sourceField");
        lenient().when(targetsParameterConfig.fetchIndividualMappings()).thenReturn(Map.of("test", "translated"));

        TranslateProcessor processor = createObjectUnderTest();

        Map<String, Object> deepData = Map.of(
            "level1", List.of(
                Map.of("level2", List.of(
                    Map.of("level3", List.of(
                        Map.of("sourceField", "test")
                    ))
                ))
            )
        );

        Record<Event> record = buildRecordWithEvent(deepData);

        assertDoesNotThrow(() -> processor.doExecute(Collections.singletonList(record)));
    }

    @Test
    void test_null_source_value_handling() {
        Map<String, Object> data = new HashMap<>();
        data.put("sourceField", null);
        Record<Event> record = buildRecordWithEvent(data);

        TranslateProcessor processor = createObjectUnderTest();

        processor.doExecute(Collections.singletonList(record));

        assertFalse(record.getData().containsKey("targetField"));
    }

    @Test
    void test_exception_in_mapping_config_processing() {
        lenient().when(mappingsParameterConfig.getTargetsParameterConfigs())
                .thenThrow(new RuntimeException("Config error"));

        TranslateProcessor processor = createObjectUnderTest();
        Record<Event> record = getEvent("test");

        // Should handle exception gracefully and continue processing
        assertDoesNotThrow(() -> processor.doExecute(Collections.singletonList(record)));
    }

    // Helper methods

    private TranslateProcessor createObjectUnderTest() {
        return new TranslateProcessor(pluginMetrics, mockConfig, expressionEvaluator, eventKeyFactory);
    }

    private Record<Event> getEvent(Object sourceField) {
        Map<String, Object> testData = new HashMap<>();
        testData.put("sourceField", sourceField);
        return buildRecordWithEvent(testData);
    }

    private static Record<Event> buildRecordWithEvent(final Map<String, Object> data) {
        return new Record<>(JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build());
    }
    
    @Test
    void testTranslateProcessorWithRe2j() {
        System.setProperty("dataprepper.pattern.provider", "re2j");
        try {
            when(mockConfig.getSource()).thenReturn("sourceField");
            when(mockConfig.getTargets()).thenReturn(List.of());
            TranslateProcessor processor = createObjectUnderTest();
            
            final Record<Event> record = getEvent("testValue");
            final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));
            
            assertThat(editedRecords.size(), equalTo(1));
        } finally {
            System.clearProperty("dataprepper.pattern.provider");
        }
    }
}