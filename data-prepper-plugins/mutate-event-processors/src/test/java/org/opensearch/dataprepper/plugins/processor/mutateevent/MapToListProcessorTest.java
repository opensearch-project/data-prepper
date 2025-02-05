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
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.record.Record;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MapToListProcessorTest {
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private MapToListProcessorConfig mockConfig;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    @BeforeEach
    void setUp() {
        lenient().when(mockConfig.getSource()).thenReturn("my-map");
        lenient().when(mockConfig.getTarget()).thenReturn("my-list");
        lenient().when(mockConfig.getKeyName()).thenReturn("key");
        lenient().when(mockConfig.getValueName()).thenReturn("value");
        lenient().when(mockConfig.getMapToListWhen()).thenReturn(null);
        lenient().when(mockConfig.getExcludeKeys()).thenReturn(new ArrayList<>());
        lenient().when(mockConfig.getRemoveProcessedFields()).thenReturn(false);
        lenient().when(mockConfig.getConvertFieldToList()).thenReturn(false);
        lenient().when(mockConfig.getTagsOnFailure()).thenReturn(new ArrayList<>());
    }

    @Test
    void invalid_map_to_list_when_throws_InvalidPluginConfigurationException() {
        final String mapToListWhen = UUID.randomUUID().toString();
        when(mockConfig.getMapToListWhen()).thenReturn(mapToListWhen);

        when(expressionEvaluator.isValidExpressionStatement(mapToListWhen)).thenReturn(false);

        assertThrows(InvalidPluginConfigurationException.class, this::createObjectUnderTest);
    }

    @Test
    void testMapToListSuccessWithDefaultOptions() {

        final MapToListProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord();
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        List<Map<String, Object>> resultList = resultEvent.get("my-list", List.class);

        assertThat(resultList.size(), is(3));
        assertThat(resultList, containsInAnyOrder(
                Map.of("key", "key1", "value", "value1"),
                Map.of("key", "key2", "value", "value2"),
                Map.of("key", "key3", "value", "value3")
        ));
        assertThat(resultEvent.containsKey("my-map"), is(true));
        assertSourceMapUnchanged(resultEvent);
    }

    @Test
    void testMapToListSuccessWithNestedMap() {

        final MapToListProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecordWithNestedMap();
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        List<Map<String, Object>> resultList = resultEvent.get("my-list", List.class);

        assertThat(resultList.size(), is(2));
        assertThat(resultList, containsInAnyOrder(
                Map.of("key", "key1", "value", "value1"),
                Map.of("key", "key2", "value", Map.of("key2-1", "value2"))
        ));
    }

    @Test
    void testMapToListSuccessWithRootAsSource() {
        when(mockConfig.getSource()).thenReturn("");

        final MapToListProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createFlatTestRecord();
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        List<Map<String, Object>> resultList = resultEvent.get("my-list", List.class);

        assertThat(resultList.size(), is(3));
        assertThat(resultList, containsInAnyOrder(
                Map.of("key", "key1", "value", "value1"),
                Map.of("key", "key2", "value", "value2"),
                Map.of("key", "key3", "value", "value3")
        ));
        assertSourceMapUnchangedForFlatRecord(resultEvent);
    }

    @Test
    void testMapToListSuccessWithCustomKeyNameValueName() {
        final String keyName = "custom-key-name";
        final String valueName = "custom-value-name";
        when(mockConfig.getKeyName()).thenReturn(keyName);
        when(mockConfig.getValueName()).thenReturn(valueName);

        final MapToListProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord();
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        List<Map<String, Object>> resultList = resultEvent.get("my-list", List.class);

        assertThat(resultList.size(), is(3));
        assertThat(resultList, containsInAnyOrder(
                Map.of(keyName, "key1", valueName, "value1"),
                Map.of(keyName, "key2", valueName, "value2"),
                Map.of(keyName, "key3", valueName, "value3")
        ));
        assertSourceMapUnchanged(resultEvent);
    }

    @Test
    void testEventNotProcessedWhenSourceNotExistInEvent() {
        when(mockConfig.getSource()).thenReturn("my-other-map");

        final MapToListProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord();
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        assertThat(resultEvent.containsKey("my-list"), is(false));
        assertSourceMapUnchanged(resultEvent);
    }

    @Test
    void testExcludedKeysAreNotProcessed() {
        when(mockConfig.getExcludeKeys()).thenReturn(List.of("key1", "key3", "key5"));

        final MapToListProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord();
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        List<Map<String, Object>> resultList = resultEvent.get("my-list", List.class);

        assertThat(resultList.size(), is(1));
        assertThat(resultList.get(0), is(Map.of("key", "key2", "value", "value2")));
        assertSourceMapUnchanged(resultEvent);
    }

    @Test
    void testExcludedKeysAreNotProcessedWithRootAsSource() {
        when(mockConfig.getSource()).thenReturn("");
        when(mockConfig.getExcludeKeys()).thenReturn(List.of("key1", "key3", "key5"));

        final MapToListProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createFlatTestRecord();
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        List<Map<String, Object>> resultList = resultEvent.get("my-list", List.class);

        assertThat(resultList.size(), is(1));
        assertThat(resultList.get(0), is(Map.of("key", "key2", "value", "value2")));
        assertSourceMapUnchangedForFlatRecord(resultEvent);
    }

    @Test
    void testRemoveProcessedFields() {
        when(mockConfig.getExcludeKeys()).thenReturn(List.of("key1", "key3", "key5"));
        when(mockConfig.getRemoveProcessedFields()).thenReturn(true);

        final MapToListProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord();
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        List<Map<String, Object>> resultList = resultEvent.get("my-list", List.class);

        assertThat(resultList.size(), is(1));
        assertThat(resultList.get(0), is(Map.of("key", "key2", "value", "value2")));

        assertThat(resultEvent.containsKey("my-map"), is(true));
        assertThat(resultEvent.containsKey("my-map/key1"), is(true));
        assertThat(resultEvent.get("my-map/key1", String.class), is("value1"));
        assertThat(resultEvent.containsKey("my-map/key2"), is(false));
        assertThat(resultEvent.containsKey("my-map/key3"), is(true));
        assertThat(resultEvent.get("my-map/key3", String.class), is("value3"));
    }

    @Test
    void testRemoveProcessedFieldsWithRootAsSource() {
        when(mockConfig.getSource()).thenReturn("");
        when(mockConfig.getExcludeKeys()).thenReturn(List.of("key1", "key3", "key5"));
        when(mockConfig.getRemoveProcessedFields()).thenReturn(true);

        final MapToListProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createFlatTestRecord();
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        List<Map<String, Object>> resultList = resultEvent.get("my-list", List.class);

        assertThat(resultList.size(), is(1));
        assertThat(resultList.get(0), is(Map.of("key", "key2", "value", "value2")));

        assertThat(resultEvent.containsKey("key1"), is(true));
        assertThat(resultEvent.get("key1", String.class), is("value1"));
        assertThat(resultEvent.containsKey("key2"), is(false));
        assertThat(resultEvent.containsKey("key3"), is(true));
        assertThat(resultEvent.get("key3", String.class), is("value3"));
    }

    @Test
    public void testConvertFieldToListSuccess() {
        when(mockConfig.getConvertFieldToList()).thenReturn(true);

        final MapToListProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord();
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        List<List<Object>> resultList = resultEvent.get("my-list", List.class);

        assertThat(resultList.size(), is(3));
        assertThat(resultList, containsInAnyOrder(
                List.of("key1", "value1"),
                List.of("key2", "value2"),
                List.of("key3", "value3")
        ));
        assertSourceMapUnchanged(resultEvent);
    }

    @Test
    public void testConvertFieldToListSuccessWithNestedMap() {
        when(mockConfig.getConvertFieldToList()).thenReturn(true);

        final MapToListProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecordWithNestedMap();
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        List<List<Object>> resultList = resultEvent.get("my-list", List.class);

        assertThat(resultList.size(), is(2));
        assertThat(resultList, containsInAnyOrder(
                List.of("key1", "value1"),
                List.of("key2", Map.of("key2-1", "value2"))
        ));
    }

    @Test
    public void testConvertFieldToListSuccessWithRootAsSource() {
        when(mockConfig.getSource()).thenReturn("");
        when(mockConfig.getConvertFieldToList()).thenReturn(true);

        final MapToListProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createFlatTestRecord();
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        List<List<Object>> resultList = resultEvent.get("my-list", List.class);

        assertThat(resultList.size(), is(3));
        assertThat(resultList, containsInAnyOrder(
                List.of("key1", "value1"),
                List.of("key2", "value2"),
                List.of("key3", "value3")
        ));
        assertSourceMapUnchangedForFlatRecord(resultEvent);
    }

    @Test
    public void testEventNotProcessedWhenTheWhenConditionIsFalse() {
        final String whenCondition = UUID.randomUUID().toString();
        when(mockConfig.getMapToListWhen()).thenReturn(whenCondition);
        when(expressionEvaluator.isValidExpressionStatement(whenCondition)).thenReturn(true);

        final MapToListProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord();
        when(expressionEvaluator.evaluateConditional(whenCondition, testRecord.getData())).thenReturn(false);
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        assertThat(resultEvent.containsKey("my-list"), is(false));
        assertSourceMapUnchanged(resultEvent);
    }

    @Test
    void testFailureTagsAreAddedWhenException() {
        // non-existing source key
        when(mockConfig.getSource()).thenReturn("my-other-map");
        final List<String> testTags = List.of("tag1", "tag2");
        when(mockConfig.getTagsOnFailure()).thenReturn(testTags);

        final MapToListProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord();
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        assertThat(resultEvent.containsKey("my-list"), is(false));
        assertSourceMapUnchanged(resultEvent);
        assertThat(resultEvent.getMetadata().getTags(), is(new HashSet<>(testTags)));
    }

    @Test
    void testMapToListSuccessWithNullValuesInMap() {

        final MapToListProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecordWithNullValues();
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        List<Map<String, Object>> resultList = resultEvent.get("my-list", List.class);

        assertThat(resultList.size(), is(2));
        Map<String, Object> resultMapWithNullValue = new HashMap<>();
        resultMapWithNullValue.put("key", "key2");
        resultMapWithNullValue.put("value", null);
        assertThat(resultList, containsInAnyOrder(
                Map.of("key", "key1", "value", "value1"),
                resultMapWithNullValue
        ));
        assertThat(resultEvent.containsKey("my-map"), is(true));
        assertSourceMapUnchangedWithNullValues(resultEvent);
    }

    @Test
    public void testConvertFieldToListSuccessWithNullValuesInMap() {
        when(mockConfig.getConvertFieldToList()).thenReturn(true);

        final MapToListProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecordWithNullValues();
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        List<List<Object>> resultList = resultEvent.get("my-list", List.class);

        assertThat(resultList.size(), is(2));
        assertThat(resultList, containsInAnyOrder(
                Arrays.asList("key1", "value1"),
                Arrays.asList("key2", null)
        ));
        assertSourceMapUnchangedWithNullValues(resultEvent);
    }
    @Test
    void invalid_map_to_list_when_with_entries_format_throws_InvalidPluginConfigurationException() {
        when(mockConfig.getSource()).thenReturn(null);

        final List<MapToListProcessorConfig.Entry> entries = List.of(
                new MapToListProcessorConfig.Entry(
                        "my-map",
                        "my-list",
                        "key",
                        "value",
                        false,
                        false,
                        new ArrayList<>(),
                        null,
                        "invalid_condition"
                )
        );

        when(mockConfig.getEntries()).thenReturn(entries);
        when(expressionEvaluator.isValidExpressionStatement("invalid_condition")).thenReturn(false);

        assertThrows(InvalidPluginConfigurationException.class, this::createObjectUnderTest);
    }

    @Test
    void testMultipleEntriesFormatWithDifferentConditions() {
        when(mockConfig.getSource()).thenReturn(null);

        final List<MapToListProcessorConfig.Entry> entries = Arrays.asList(
                new MapToListProcessorConfig.Entry(
                        "my-map",
                        "target1",
                        "key",
                        "value",
                        false,
                        false,
                        new ArrayList<>(),
                        null,
                        "condition1"
                ),
                new MapToListProcessorConfig.Entry(
                        "my-map2",
                        "target2",
                        "key2",
                        "value2",
                        false,
                        false,
                        new ArrayList<>(),
                        null,
                        "condition2"
                )
        );

        when(mockConfig.getEntries()).thenReturn(entries);
        when(expressionEvaluator.isValidExpressionStatement("condition1")).thenReturn(true);
        when(expressionEvaluator.isValidExpressionStatement("condition2")).thenReturn(true);

        final Record<Event> testRecord = createTestRecordWithMultipleMaps();
        when(expressionEvaluator.evaluateConditional("condition1", testRecord.getData())).thenReturn(true);
        when(expressionEvaluator.evaluateConditional("condition2", testRecord.getData())).thenReturn(false);

        final MapToListProcessor processor = createObjectUnderTest();
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        List<List<Object>> resultList = resultEvent.get("target1", List.class);

        assertThat(resultList.size(), is(3));
        assertThat(resultList, containsInAnyOrder(
                Map.of("key", "key1", "value", "value1"),
                Map.of("key", "key2", "value", "value2"),
                Map.of("key", "key3", "value", "value3")
        ));
        assertSourceMapUnchanged(resultEvent);
    }

    @Test
    public void test_both_configurations_used_together() {
        final MapToListProcessorConfig configObjectUnderTest = new MapToListProcessorConfig();
        ReflectionTestUtils.setField(configObjectUnderTest, "source", "my-map");

        final MapToListProcessorConfig.Entry entry = new MapToListProcessorConfig.Entry(
                "my-map2",
                "target2",
                "key",
                "value",
                false,
                false,
                new ArrayList<>(),
                null,
                "condition"
        );

        ReflectionTestUtils.setField(configObjectUnderTest, "entries", List.of(entry));

        assertThat(configObjectUnderTest.isNotUsingBothConfigurations(), is(false));
    }

    @Test
    public void test_no_configuration_used() {
        final MapToListProcessorConfig configObjectUnderTest = new MapToListProcessorConfig();

        ReflectionTestUtils.setField(configObjectUnderTest, "source", null);
        ReflectionTestUtils.setField(configObjectUnderTest, "entries", null);

        assertThat(configObjectUnderTest.isUsingAtLeastOneConfiguration(), is(false));
    }

    private MapToListProcessor createObjectUnderTest() {
        return new MapToListProcessor(pluginMetrics, mockConfig, expressionEvaluator);
    }

    private Record<Event> createTestRecord() {
        final Map<String, Map<String, Object>> data = Map.of("my-map", Map.of(
                "key1", "value1",
                "key2", "value2",
                "key3", "value3"));
        final Event event = JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build();
        return new Record<>(event);
    }

    private Record<Event> createFlatTestRecord() {
        final Map<String, String> data = Map.of(
            "key1", "value1",
            "key2", "value2",
            "key3", "value3");
        final Event event = JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build();
        return new Record<>(event);
    }

    private Record<Event> createTestRecordWithNestedMap() {
        final Map<String, Map<String, Object>> data = Map.of("my-map", Map.of(
                "key1", "value1",
                "key2", Map.of("key2-1", "value2")));
        final Event event = JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build();
        return new Record<>(event);
    }

    private Record<Event> createTestRecordWithNullValues() {
        final Map<String, Object> mapData = new HashMap<>();
        mapData.put("key1", "value1");
        mapData.put("key2", null);
        final Map<String, Map<String, Object>> data = Map.of("my-map", mapData);
        final Event event = JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build();
        return new Record<>(event);
    }

    private Record<Event> createTestRecordWithMultipleMaps() {
        final Map<String, Map<String, Object>> data = Map.of(
                "my-map", Map.of(
                        "key1", "value1",
                        "key2", "value2",
                        "key3", "value3"),
                "my-map2", Map.of(
                        "key4", "value4",
                        "key5", "value5",
                        "key6", "value6"));
        final Event event = JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build();
        return new Record<>(event);
    }

    private void assertSourceMapUnchanged(final Event resultEvent) {
        assertThat(resultEvent.containsKey("my-map"), is(true));
        assertThat(resultEvent.get("my-map/key1", String.class), is("value1"));
        assertThat(resultEvent.get("my-map/key2", String.class), is("value2"));
        assertThat(resultEvent.get("my-map/key3", String.class), is("value3"));
    }

    private void assertSourceMapUnchangedForFlatRecord(final Event resultEvent) {
        assertThat(resultEvent.get("key1", String.class), is("value1"));
        assertThat(resultEvent.get("key2", String.class), is("value2"));
        assertThat(resultEvent.get("key3", String.class), is("value3"));
    }

    private void assertSourceMapUnchangedWithNullValues(final Event resultEvent) {
        assertThat(resultEvent.containsKey("my-map"), is(true));
        assertThat(resultEvent.get("my-map/key1", String.class), is("value1"));
        assertThat(resultEvent.get("my-map/key2", String.class), nullValue());
    }
}
