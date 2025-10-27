package org.opensearch.dataprepper.plugins.processor.translate;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.event.TestEventKeyFactory;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventKeyFactory;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
class TranslateProcessorJsonConfigTest {

    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private ExpressionEvaluator expressionEvaluator;

    private final EventKeyFactory eventKeyFactory = TestEventKeyFactory.getTestEventFactory();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void test_static_key_translation() throws IOException {
        TranslateProcessor processor = createProcessor("translate_static_key.json");
        Record<Event> record = getEvent("key1");
        List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", String.class), is("mappedValue1"));
    }

    @ParameterizedTest
    @CsvSource({"admin,mappedValue1", "user,mappedValue2"})
    void test_dynamic_key_translation(String replaceValue, String valueToAssert) throws IOException {
        TranslateProcessor processor = createProcessor("translate_dynamic_key.json");
        Map<String, Object> data = new HashMap<>();
        data.put("type", replaceValue);
        data.put("user." + replaceValue + ".id", replaceValue);
        Record<Event> record = buildRecordWithEvent(data);

        List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));
        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertEquals(valueToAssert, translatedRecords.get(0).getData().get("targetField", String.class));
    }

    @Test
    void test_pattern_matching() throws IOException {
        TranslateProcessor processor = createProcessor("translate_pattern_matching.json");
        Record<Event> record = getEvent("key1");
        List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", String.class), is("matched_single_digit"));
    }

    @Test
    void test_range_mapping() throws IOException {
        TranslateProcessor processor = createProcessor("translate_range_mapping.json");
        Record<Event> record = getEvent("15");
        List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertTrue(translatedRecords.get(0).getData().containsKey("targetField"));
        assertThat(translatedRecords.get(0).getData().get("targetField", String.class), is("mappedValue2"));
    }

    @Test
    void test_nested_path_translation() throws IOException {
        TranslateProcessor processor = createProcessor("translate_nested_path.json");
        Map<String, Object> testJson = new HashMap<>();
        testJson.put("collection", new ArrayList<>(List.of(
                Map.of("sourceField", "key1"),
                Map.of("sourceField", "key2"))));
        Record<Event> record = buildRecordWithEvent(testJson);

        List<Record<Event>> translatedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        List<Map<String, Object>> expected = new ArrayList<>(List.of(
                Map.of("sourceField", "key1", "targetField", "mappedValue1"),
                Map.of("sourceField", "key2", "targetField", "mappedValue2")));
        assertThat(translatedRecords.get(0).getData().get("collection", ArrayList.class), is(expected));
    }

    private TranslateProcessorConfig loadConfig(String jsonFile) throws IOException {
        try (InputStream is = getClass().getResourceAsStream("/configs/" + jsonFile)) {
            if (is == null) {
                throw new IOException("Config file not found: " + jsonFile);
            }
            TranslateProcessorConfig config = objectMapper.readValue(is, TranslateProcessorConfig.class);
            config.hasMappings(); // Trigger parsing of mappings
            for (MappingsParameterConfig mappingConfig : config.getCombinedMappingsConfigs()) {
                mappingConfig.parseMappings(); // Parse each mapping configuration
            }
            return config;
        }
    }

    private TranslateProcessor createProcessor(String configFile) throws IOException {
        return new TranslateProcessor(
                pluginMetrics,
                loadConfig(configFile),
                expressionEvaluator,
                eventKeyFactory
        );
    }

    @Nested
    class KeyResolutionTests {
        @Test
        void test_static_key_caching() throws IOException {
            TranslateProcessor processor = createProcessor("translate_static_key.json");

            // First execution
            List<Record<Event>> firstResult = (List<Record<Event>>) processor.doExecute(
                    Collections.singletonList(getEvent("key1")));
            assertThat(firstResult.get(0).getData().get("targetField", String.class), is("mappedValue1"));

            // Second execution should use cached keys and produce same result
            List<Record<Event>> secondResult = (List<Record<Event>>) processor.doExecute(
                    Collections.singletonList(getEvent("key1")));
            assertThat(secondResult.get(0).getData().get("targetField", String.class), is("mappedValue1"));
        }

        @Test
        void test_dynamic_key_resolution() throws IOException {
            TranslateProcessor processor = createProcessor("translate_dynamic_key.json");

            // First call
            Map<String, Object> data1 = new HashMap<>();
            data1.put("type", "admin");
            data1.put("user.admin.id", "admin");

            // Second call with different type
            Map<String, Object> data2 = new HashMap<>();
            data2.put("type", "user");
            data2.put("user.user.id", "user");
            Collection<Record<Event>> output = processor.doExecute(List.of(buildRecordWithEvent(data1), buildRecordWithEvent(data2)));

            // Verify dynamic keys are resolved each time
            assertEquals(2, output.size());
            List<Record<Event>> outputRecords = (List<Record<Event>>) output;
            assertEquals("mappedValue1", outputRecords.get(0).getData().get("targetField", String.class));
            assertEquals("mappedValue2", outputRecords.get(1).getData().get("targetField", String.class));
        }
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
}