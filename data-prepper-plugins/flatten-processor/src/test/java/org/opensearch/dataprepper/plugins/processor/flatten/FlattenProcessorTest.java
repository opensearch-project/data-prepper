/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.flatten;

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
import org.opensearch.dataprepper.model.event.FieldConflictStrategy;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.record.Record;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FlattenProcessorTest {
    private static final String SOURCE_KEY = "source";
    private static final String TARGET_KEY = "target";

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private FlattenProcessorConfig mockConfig;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    @BeforeEach
    void setUp() {
        lenient().when(mockConfig.getSource()).thenReturn("");
        lenient().when(mockConfig.getTarget()).thenReturn("");
        lenient().when(mockConfig.isRemoveProcessedFields()).thenReturn(false);
        lenient().when(mockConfig.isRemoveListIndices()).thenReturn(false);
        lenient().when(mockConfig.isRemoveBrackets()).thenReturn(false);
        lenient().when(mockConfig.getFlattenWhen()).thenReturn(null);
        lenient().when(mockConfig.getTagsOnFailure()).thenReturn(new ArrayList<>());
        lenient().when(mockConfig.getExcludeKeys()).thenReturn(new ArrayList<>());
        lenient().when(mockConfig.getFlattenSeparator()).thenReturn(".");
        lenient().when(mockConfig.getTargetConflictStrategy()).thenReturn(FieldConflictStrategy.OVERWRITE);
    }

    @Test
    void invalid_flatten_when_expression_throws_InvalidPluginConfigurationException() {
        final String flattenWhen = UUID.randomUUID().toString();

        when(mockConfig.getFlattenWhen()).thenReturn(flattenWhen);

        when(expressionEvaluator.isValidExpressionStatement(flattenWhen)).thenReturn(false);

        assertThrows(InvalidPluginConfigurationException.class, this::createObjectUnderTest);
    }

    @Test
    void testFlattenEntireEventData() {
        final FlattenProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord(createTestData());
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        Map<String, Object> resultData = resultEvent.get("", Map.class);

        assertFlattenedData(resultData);

        assertThat(resultData.containsKey("key2"), is(true));
        assertThat(resultData.containsKey("list1"), is(true));
    }

    @Test
    void testFlattenEntireEventDataAndRemoveProcessedFields() {
        when(mockConfig.isRemoveProcessedFields()).thenReturn(true);

        final FlattenProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord(createTestData());
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        Map<String, Object> resultData = resultEvent.get("", Map.class);

        assertFlattenedData(resultData);

        assertThat(resultData.containsKey("key2"), is(false));
        assertThat(resultData.containsKey("list1"), is(false));
    }

    @Test
    void testFlattenEntireEventDataAndRemoveListIndices() {
        when(mockConfig.isRemoveListIndices()).thenReturn(true);

        final FlattenProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord(createTestData());
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        Map<String, Object> resultData = resultEvent.get("", Map.class);

        assertThat(resultData.containsKey("key1"), is(true));
        assertThat(resultData.get("key1"), is("val1"));

        assertThat(resultData.containsKey("key1"), is(true));
        assertThat(resultData.get("key2.key3.key.4"), is("val2"));

        assertThat(resultData.containsKey("list1[].list2[].name"), is(true));
        assertThat(resultData.get("list1[].list2[].name"), is(List.of("name1", "name2")));

        assertThat(resultData.containsKey("list1[].list2[].value"), is(true));
        assertThat(resultData.get("list1[].list2[].value"), is(List.of("value1", "value2")));
    }

    @Test
    void testFlattenEntireEventDataAndRemoveListIndicesAndRemoveBrackets() {
        when(mockConfig.isRemoveListIndices()).thenReturn(true);
        when(mockConfig.isRemoveBrackets()).thenReturn(true);

        final FlattenProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord(createTestData());
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        Map<String, Object> resultData = resultEvent.get("", Map.class);

        assertThat(resultData.containsKey("key1"), is(true));
        assertThat(resultData.get("key1"), is("val1"));

        assertThat(resultData.containsKey("key1"), is(true));
        assertThat(resultData.get("key2.key3.key.4"), is("val2"));

        assertThat(resultData.containsKey("list1[].list2[].name"), is(false));
        assertThat(resultData.containsKey("list1.list2.name"), is(true));
        assertThat(resultData.get("list1.list2.name"), is(List.of("name1", "name2")));

        assertThat(resultData.containsKey("list1[].list2[].value"), is(false));
        assertThat(resultData.containsKey("list1.list2.value"), is(true));
        assertThat(resultData.get("list1.list2.value"), is(List.of("value1", "value2")));
    }

    @Test
    void testFlattenWithSpecificFieldsAsSourceAndTarget() {
        when(mockConfig.getSource()).thenReturn(SOURCE_KEY);
        when(mockConfig.getTarget()).thenReturn(TARGET_KEY);

        final FlattenProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord(Map.of(SOURCE_KEY, createTestData()));
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        Map<String, Object> resultData = resultEvent.get(TARGET_KEY, Map.class);

        assertFlattenedData(resultData);
    }

    @Test
    void testFlattenWithSpecificFieldsAsSourceAndTargetAndRemoveProcessedFields() {
        when(mockConfig.getSource()).thenReturn(SOURCE_KEY);
        when(mockConfig.getTarget()).thenReturn(TARGET_KEY);
        when(mockConfig.isRemoveProcessedFields()).thenReturn(true);

        final FlattenProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord(Map.of(SOURCE_KEY, createTestData()));
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        Map<String, Object> resultData = resultEvent.get(TARGET_KEY, Map.class);

        assertFlattenedData(resultData);

        Map<String, Object> sourceData = resultEvent.get(SOURCE_KEY, Map.class);
        assertThat(sourceData.containsKey("key1"), is(false));
        assertThat(sourceData.containsKey("key2"), is(false));
        assertThat(sourceData.containsKey("list1"), is(false));
    }

    @Test
    void testFlattenWithSpecificFieldsAsSourceAndTargetAndRemoveListIndices() {
        when(mockConfig.getSource()).thenReturn(SOURCE_KEY);
        when(mockConfig.getTarget()).thenReturn(TARGET_KEY);
        when(mockConfig.isRemoveListIndices()).thenReturn(true);

        final FlattenProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord(Map.of(SOURCE_KEY, createTestData()));
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        Map<String, Object> resultData = resultEvent.get(TARGET_KEY, Map.class);

        assertThat(resultData.containsKey("key1"), is(true));
        assertThat(resultData.get("key1"), is("val1"));

        assertThat(resultData.containsKey("key1"), is(true));
        assertThat(resultData.get("key2.key3.key.4"), is("val2"));

        assertThat(resultData.containsKey("list1[].list2[].name"), is(true));
        assertThat(resultData.get("list1[].list2[].name"), is(List.of("name1", "name2")));

        assertThat(resultData.containsKey("list1[].list2[].value"), is(true));
        assertThat(resultData.get("list1[].list2[].value"), is(List.of("value1", "value2")));
    }

    @Test
    void testFlattenWithSpecificFieldsAsSourceAndTargetAndRemoveListIndicesAndRemoveBrackets() {
        when(mockConfig.getSource()).thenReturn(SOURCE_KEY);
        when(mockConfig.getTarget()).thenReturn(TARGET_KEY);
        when(mockConfig.isRemoveListIndices()).thenReturn(true);
        when(mockConfig.isRemoveBrackets()).thenReturn(true);

        final FlattenProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord(Map.of(SOURCE_KEY, createTestData()));
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        Map<String, Object> resultData = resultEvent.get(TARGET_KEY, Map.class);

        assertThat(resultData.containsKey("key1"), is(true));
        assertThat(resultData.get("key1"), is("val1"));

        assertThat(resultData.containsKey("key1"), is(true));
        assertThat(resultData.get("key2.key3.key.4"), is("val2"));

        assertThat(resultData.containsKey("list1[].list2[].name"), is(false));
        assertThat(resultData.containsKey("list1.list2.name"), is(true));
        assertThat(resultData.get("list1.list2.name"), is(List.of("name1", "name2")));

        assertThat(resultData.containsKey("list1[].list2[].value"), is(false));
        assertThat(resultData.containsKey("list1.list2.value"), is(true));
        assertThat(resultData.get("list1.list2.value"), is(List.of("value1", "value2")));
    }

    @Test
    public void testEventNotProcessedWhenTheWhenConditionIsFalse() {
        final String whenCondition = UUID.randomUUID().toString();
        when(mockConfig.getFlattenWhen()).thenReturn(whenCondition);
        when(expressionEvaluator.isValidExpressionStatement(whenCondition)).thenReturn(true);

        final FlattenProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord(createTestData());
        when(expressionEvaluator.evaluateConditional(whenCondition, testRecord.getData())).thenReturn(false);
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        Map<String, Object> resultData = resultEvent.get("", Map.class);

        assertThat(resultData.containsKey("key2.key3.key.4"), is(false));
        assertThat(resultData.containsKey("list1[0].list2[0].name"), is(false));
        assertThat(resultData.containsKey("list1[0].list2[0].value"), is(false));
        assertThat(resultData.containsKey("list1[0].list2[1].name"), is(false));
        assertThat(resultData.containsKey("list1[0].list2[1].value"), is(false));
    }

    @Test
    void testFailureTagsAreAddedWhenException() {
        // non-existing source key
        when(mockConfig.getSource()).thenReturn("my-other-map");
        final List<String> testTags = List.of("tag1", "tag2");
        when(mockConfig.getTagsOnFailure()).thenReturn(testTags);

        final FlattenProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord(createTestData());
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        assertThat(resultEvent.getMetadata().getTags(), is(new HashSet<>(testTags)));
    }

    @ParameterizedTest
    @MethodSource("excludeKeysTestArguments")
    void testFlattenWithExcludeKeys(String source, String target, List<String> excludeKeys, Map<String, Object> expectedResultMap) {
        when(mockConfig.getSource()).thenReturn(source);
        when(mockConfig.getTarget()).thenReturn(target);
        when(mockConfig.isRemoveProcessedFields()).thenReturn(true);
        when(mockConfig.isRemoveListIndices()).thenReturn(true);
        when(mockConfig.getExcludeKeys()).thenReturn(excludeKeys);

        final FlattenProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord(createTestData());
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();

        assertThat(resultEvent.toMap(), is(expectedResultMap));
    }

    @Test
    void testFlattenWithExcludeKeysEventNotChangedWhenException() {
        when(mockConfig.isRemoveListIndices()).thenThrow(RuntimeException.class);
        when(mockConfig.getExcludeKeys()).thenReturn(List.of("key1", "key2"));

        final FlattenProcessor processor = createObjectUnderTest();
        final Map<String, Object> testData = createTestData();
        final Record<Event> testRecord = createTestRecord(testData);
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();

        assertThat(resultEvent.toMap(), is(testData));
    }

    private static Stream<Arguments> excludeKeysTestArguments() {
        return Stream.of(
                // source and target are the same, single exclude key
                Arguments.of("", "", List.of("list1"), Map.of(
                        "key1", "val1",
                        "key2.key3.key.4", "val2",
                        "list1", List.of(
                                Map.of("list2", List.of(
                                        Map.of("name", "name1", "value", "value1"),
                                        Map.of("name", "name2", "value", "value2"))
                                )
                        )
                )),
                // source and target are the same, exclude keys contain nonexistent key
                Arguments.of("", "", List.of("list1", "no_such_key"), Map.of(
                        "key1", "val1",
                        "key2.key3.key.4", "val2",
                        "list1", List.of(
                                Map.of("list2", List.of(
                                        Map.of("name", "name1", "value", "value1"),
                                        Map.of("name", "name2", "value", "value2"))
                                )
                        )
                )),
                // source and target are different, single exclude key
                Arguments.of("", "unmapped", List.of("key1"), Map.of(
                        "key1", "val1",
                        "unmapped", Map.of(
                                "key2.key3.key.4", "val2",
                                "list1[].list2[].name", List.of("name1", "name2"),
                                "list1[].list2[].value", List.of("value1", "value2"))
                )),
                // source and target are different, multiple exclude keys
                Arguments.of("", "unmapped", List.of("key1", "list1"), Map.of(
                        "key1", "val1",
                        "list1", List.of(
                                Map.of("list2", List.of(
                                        Map.of("name", "name1", "value", "value1"),
                                        Map.of("name", "name2", "value", "value2"))
                                )
                        ),
                        "unmapped", Map.of("key2.key3.key.4", "val2")
                )),
                // source and target are different, multiple exclude keys with json pointers
                Arguments.of("", "unmapped", List.of("key1", "key2/key3"), Map.of(
                        "key1", "val1",
                        "key2", Map.of("key3", Map.of("key.4", "val2")),
                        "unmapped", Map.of(
                                "key2", Map.of(),
                                "list1[].list2[].name", List.of("name1", "name2"),
                                "list1[].list2[].value", List.of("value1", "value2"))
                )),
                // source is not root
                Arguments.of("key2", "unmapped", List.of("key3"), Map.of(
                        "key1", "val1",
                        "key2", Map.of("key3", Map.of("key.4", "val2")),
                        "list1", List.of(
                                Map.of("list2", List.of(
                                        Map.of("name", "name1", "value", "value1"),
                                        Map.of("name", "name2", "value", "value2"))
                                )
                        ),
                        "unmapped", Map.of()
                ))
        );
    }

    private FlattenProcessor createObjectUnderTest() {
        return new FlattenProcessor(pluginMetrics, mockConfig, expressionEvaluator);
    }

    private Map<String, Object> createTestData() {
        // Json data:
        // {
        //  "key1": "val1",
        //  "key2": {
        //    "key3": {
        //      "key.4": "val2"
        //    }
        //  },
        //  "list1": [
        //    {
        //      "list2": [
        //        {
        //          "name": "name1",
        //          "value": "value1"
        //        },
        //        {
        //          "name": "name2",
        //          "value": "value2"
        //        }
        //      ]
        //    }
        //  ]
        //}
        return Map.of(
                "key1", "val1",
                "key2", Map.of("key3", Map.of("key.4", "val2")),
                "list1", List.of(
                        Map.of("list2", List.of(
                                Map.of("name", "name1", "value", "value1"),
                                Map.of("name", "name2", "value", "value2"))
                        )
                )
        );
    }

    private Record<Event> createTestRecord(Object data) {
        final Event event = JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build();
        return new Record<>(event);
    }

    private void assertFlattenedData(Map<String, Object> resultData) {
        assertThat(resultData.containsKey("key1"), is(true));
        assertThat(resultData.get("key1"), is("val1"));

        assertThat(resultData.containsKey("key2.key3.key.4"), is(true));
        assertThat(resultData.get("key2.key3.key.4"), is("val2"));

        assertThat(resultData.containsKey("list1[0].list2[0].name"), is(true));
        assertThat(resultData.get("list1[0].list2[0].name"), is("name1"));

        assertThat(resultData.containsKey("list1[0].list2[0].value"), is(true));
        assertThat(resultData.get("list1[0].list2[0].value"), is("value1"));

        assertThat(resultData.containsKey("list1[0].list2[1].name"), is(true));
        assertThat(resultData.get("list1[0].list2[1].name"), is("name2"));

        assertThat(resultData.containsKey("list1[0].list2[1].value"), is(true));
        assertThat(resultData.get("list1[0].list2[1].value"), is("value2"));
    }

    @Test
    void testFlattenWithCustomSeparator() {
        when(mockConfig.getFlattenSeparator()).thenReturn("_");

        final FlattenProcessor processor = createObjectUnderTest();

        final Map<String, Object> testData = Map.of(
                "key1", "val1",
                "key2", Map.of("key3", "val2", "key4", Map.of("key5", "val3"))
        );

        final Record<Event> testRecord = createTestRecord(testData);
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        Map<String, Object> resultData = resultEvent.get("", Map.class);

        assertThat(resultData.containsKey("key1"), is(true));
        assertThat(resultData.get("key1"), is("val1"));

        assertThat(resultData.containsKey("key2_key3"), is(true));
        assertThat(resultData.get("key2_key3"), is("val2"));

        assertThat(resultData.containsKey("key2_key4_key5"), is(true));
        assertThat(resultData.get("key2_key4_key5"), is("val3"));
    }

    @Test
    void testFlattenWithDefaultSeparatorProducesDotSeparatedKeys() {
        final FlattenProcessor processor = createObjectUnderTest();

        final Map<String, Object> testData = Map.of(
                "parent", Map.of("child", "value")
        );

        final Record<Event> testRecord = createTestRecord(testData);
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        Map<String, Object> resultData = resultEvent.get("", Map.class);

        assertThat(resultData.containsKey("parent.child"), is(true));
        assertThat(resultData.get("parent.child"), is("value"));
    }

    @Test
    void testTargetConflictStrategy_overwrite_replacesEntireTargetMap() {
        when(mockConfig.getSource()).thenReturn("kv_parsed");
        when(mockConfig.getTarget()).thenReturn("attributes");
        when(mockConfig.getTargetConflictStrategy()).thenReturn(FieldConflictStrategy.OVERWRITE);

        final FlattenProcessor processor = createObjectUnderTest();

        final Map<String, Object> existingAttributes = new HashMap<>();
        existingAttributes.put("cluster_name", "my-cluster");
        existingAttributes.put("obs_body_length", 397);

        final Map<String, Object> kvParsed = Map.of("logtype", "ws:access", "method", "GET");

        final Map<String, Object> initialData = new HashMap<>();
        initialData.put("attributes", existingAttributes);
        initialData.put("kv_parsed", kvParsed);

        final Record<Event> record = createTestRecord(initialData);
        final List<Record<Event>> results = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));
        final Event event = results.get(0).getData();

        assertThat(event.get("attributes/logtype", Object.class), is("ws:access"));
        assertThat(event.get("attributes/method", Object.class), is("GET"));
        assertThat(event.containsKey("attributes/cluster_name"), is(false));
        assertThat(event.containsKey("attributes/obs_body_length"), is(false));
    }

    @Test
    void testTargetConflictStrategy_mergePreserveExistingKeys_mergesAndPreservesConflicts() {
        when(mockConfig.getSource()).thenReturn("kv_parsed");
        when(mockConfig.getTarget()).thenReturn("attributes");
        when(mockConfig.getTargetConflictStrategy()).thenReturn(FieldConflictStrategy.MERGE_PRESERVE_EXISTING_KEYS);

        final FlattenProcessor processor = createObjectUnderTest();

        final Map<String, Object> existingAttributes = new HashMap<>();
        existingAttributes.put("cluster_name", "my-cluster");
        existingAttributes.put("method", "POST");
        existingAttributes.put("obs_body_length", 397);

        final Map<String, Object> kvParsed = Map.of("logtype", "ws:access", "method", "GET", "uri", "/health");

        final Map<String, Object> initialData = new HashMap<>();
        initialData.put("attributes", existingAttributes);
        initialData.put("kv_parsed", kvParsed);

        final Record<Event> record = createTestRecord(initialData);
        final List<Record<Event>> results = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));
        final Event event = results.get(0).getData();

        assertThat(event.get("attributes/cluster_name", Object.class), is("my-cluster"));
        assertThat(event.get("attributes/obs_body_length", Object.class), is(397));
        assertThat(event.get("attributes/method", Object.class), is("POST"));
        assertThat(event.get("attributes/logtype", Object.class), is("ws:access"));
        assertThat(event.get("attributes/uri", Object.class), is("/health"));
    }

    @Test
    void testTargetConflictStrategy_mergeOverwriteExistingKeys_mergesAndOverwritesConflicts() {
        when(mockConfig.getSource()).thenReturn("kv_parsed");
        when(mockConfig.getTarget()).thenReturn("attributes");
        when(mockConfig.getTargetConflictStrategy()).thenReturn(FieldConflictStrategy.MERGE_OVERWRITE_EXISTING_KEYS);

        final FlattenProcessor processor = createObjectUnderTest();

        final Map<String, Object> existingAttributes = new HashMap<>();
        existingAttributes.put("cluster_name", "my-cluster");
        existingAttributes.put("method", "POST");
        existingAttributes.put("obs_body_length", 397);

        final Map<String, Object> kvParsed = Map.of("logtype", "ws:access", "method", "GET", "uri", "/health");

        final Map<String, Object> initialData = new HashMap<>();
        initialData.put("attributes", existingAttributes);
        initialData.put("kv_parsed", kvParsed);

        final Record<Event> record = createTestRecord(initialData);
        final List<Record<Event>> results = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));
        final Event event = results.get(0).getData();

        assertThat(event.get("attributes/cluster_name", Object.class), is("my-cluster"));
        assertThat(event.get("attributes/obs_body_length", Object.class), is(397));
        assertThat(event.get("attributes/method", Object.class), is("GET"));
        assertThat(event.get("attributes/logtype", Object.class), is("ws:access"));
        assertThat(event.get("attributes/uri", Object.class), is("/health"));
    }

    @Test
    void testTargetConflictStrategy_skip_skipsWhenTargetExists() {
        when(mockConfig.getSource()).thenReturn("kv_parsed");
        when(mockConfig.getTarget()).thenReturn("attributes");
        when(mockConfig.getTargetConflictStrategy()).thenReturn(FieldConflictStrategy.SKIP);

        final FlattenProcessor processor = createObjectUnderTest();

        final Map<String, Object> existingAttributes = new HashMap<>();
        existingAttributes.put("cluster_name", "my-cluster");

        final Map<String, Object> kvParsed = Map.of("logtype", "ws:access");

        final Map<String, Object> initialData = new HashMap<>();
        initialData.put("attributes", existingAttributes);
        initialData.put("kv_parsed", kvParsed);

        final Record<Event> record = createTestRecord(initialData);
        final List<Record<Event>> results = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));
        final Event event = results.get(0).getData();

        assertThat(event.get("attributes/cluster_name", Object.class), is("my-cluster"));
        assertThat(event.containsKey("attributes/logtype"), is(false));
    }

    @Test
    void testTargetConflictStrategy_targetDoesNotExist_writesMapRegardlessOfStrategy() {
        when(mockConfig.getSource()).thenReturn("kv_parsed");
        when(mockConfig.getTarget()).thenReturn("attributes");
        when(mockConfig.getTargetConflictStrategy()).thenReturn(FieldConflictStrategy.MERGE_PRESERVE_EXISTING_KEYS);

        final FlattenProcessor processor = createObjectUnderTest();

        final Map<String, Object> kvParsed = Map.of("logtype", "ws:access", "method", "GET");

        final Map<String, Object> initialData = new HashMap<>();
        initialData.put("kv_parsed", kvParsed);

        final Record<Event> record = createTestRecord(initialData);
        final List<Record<Event>> results = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));
        final Event event = results.get(0).getData();

        assertThat(event.get("attributes/logtype", Object.class), is("ws:access"));
        assertThat(event.get("attributes/method", Object.class), is("GET"));
    }

    @Test
    void testTargetConflictStrategy_withRemoveProcessedFields() {
        when(mockConfig.getSource()).thenReturn("kv_parsed");
        when(mockConfig.getTarget()).thenReturn("attributes");
        when(mockConfig.isRemoveProcessedFields()).thenReturn(true);
        when(mockConfig.getTargetConflictStrategy()).thenReturn(FieldConflictStrategy.MERGE_PRESERVE_EXISTING_KEYS);

        final FlattenProcessor processor = createObjectUnderTest();

        final Map<String, Object> existingAttributes = new HashMap<>();
        existingAttributes.put("cluster_name", "my-cluster");
        existingAttributes.put("method", "POST");

        final Map<String, Object> kvParsed = new HashMap<>();
        kvParsed.put("logtype", "ws:access");
        kvParsed.put("method", "GET");

        final Map<String, Object> initialData = new HashMap<>();
        initialData.put("attributes", existingAttributes);
        initialData.put("kv_parsed", kvParsed);

        final Record<Event> record = createTestRecord(initialData);
        final List<Record<Event>> results = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));
        final Event event = results.get(0).getData();

        assertThat(event.get("attributes/cluster_name", Object.class), is("my-cluster"));
        assertThat(event.get("attributes/method", Object.class), is("POST"));
        assertThat(event.get("attributes/logtype", Object.class), is("ws:access"));
        assertThat(event.containsKey("kv_parsed/logtype"), is(false));
        assertThat(event.containsKey("kv_parsed/method"), is(false));
    }

    @Test
    void testTargetConflictStrategy_rootTarget_mergePreserveExistingKeys() {
        when(mockConfig.getSource()).thenReturn("nested");
        when(mockConfig.getTarget()).thenReturn("");
        when(mockConfig.getTargetConflictStrategy()).thenReturn(FieldConflictStrategy.MERGE_PRESERVE_EXISTING_KEYS);

        final FlattenProcessor processor = createObjectUnderTest();

        final Map<String, Object> initialData = new HashMap<>();
        initialData.put("key1", "original");
        initialData.put("existing_field", "keep");
        initialData.put("nested", Map.of("key1", "new", "key2", "value2"));

        final Record<Event> record = createTestRecord(initialData);
        final List<Record<Event>> results = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));
        final Event event = results.get(0).getData();

        assertThat(event.get("existing_field", String.class), is("keep"));
        assertThat(event.get("key1", String.class), is("original"));
        assertThat(event.get("key2", String.class), is("value2"));
    }

    @Test
    void testTargetConflictStrategy_rootTarget_skip_mergesAndPreservesExistingFields() {
        when(mockConfig.getSource()).thenReturn("nested");
        when(mockConfig.getTarget()).thenReturn("");
        when(mockConfig.getTargetConflictStrategy()).thenReturn(FieldConflictStrategy.SKIP);

        final FlattenProcessor processor = createObjectUnderTest();

        final Map<String, Object> initialData = new HashMap<>();
        initialData.put("key1", "original");
        initialData.put("existing_field", "keep");
        initialData.put("nested", Map.of("key1", "new", "key2", "value2"));

        final Record<Event> record = createTestRecord(initialData);
        final List<Record<Event>> results = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));
        final Event event = results.get(0).getData();

        // At root, SKIP merges without overwriting conflicting fields
        assertThat(event.get("existing_field", String.class), is("keep"));
        assertThat(event.get("key1", String.class), is("original"));
        assertThat(event.get("key2", String.class), is("value2"));
    }

    @Test
    void testTargetConflictStrategy_rootTarget_mergeOverwriteExistingKeys_overwritesConflictingRootFields() {
        when(mockConfig.getSource()).thenReturn("nested");
        when(mockConfig.getTarget()).thenReturn("");
        when(mockConfig.getTargetConflictStrategy()).thenReturn(FieldConflictStrategy.MERGE_OVERWRITE_EXISTING_KEYS);

        final FlattenProcessor processor = createObjectUnderTest();

        final Map<String, Object> initialData = new HashMap<>();
        initialData.put("key1", "original");
        initialData.put("existing_field", "keep");
        initialData.put("nested", Map.of("key1", "new", "key2", "value2"));

        final Record<Event> record = createTestRecord(initialData);
        final List<Record<Event>> results = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));
        final Event event = results.get(0).getData();

        assertThat(event.get("existing_field", String.class), is("keep"));
        assertThat(event.get("key1", String.class), is("new"));
        assertThat(event.get("key2", String.class), is("value2"));
    }

    @Test
    void testTargetConflictStrategy_nestedMapConflict() {
        when(mockConfig.getSource()).thenReturn("kv_parsed");
        when(mockConfig.getTarget()).thenReturn("attributes");
        when(mockConfig.getTargetConflictStrategy()).thenReturn(FieldConflictStrategy.MERGE_PRESERVE_EXISTING_KEYS);

        final FlattenProcessor processor = createObjectUnderTest();

        final Map<String, Object> existingAttributes = new HashMap<>();
        existingAttributes.put("meta.env", "prod");
        existingAttributes.put("meta.version", 1);

        final Map<String, Object> kvParsed = Map.of("meta", Map.of("version", 2, "region", "us-east-1"));

        final Map<String, Object> initialData = new HashMap<>();
        initialData.put("attributes", existingAttributes);
        initialData.put("kv_parsed", kvParsed);

        final Record<Event> record = createTestRecord(initialData);
        final List<Record<Event>> results = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));
        final Event event = results.get(0).getData();

        assertThat(event.get("attributes/meta.env", String.class), is("prod"));
        assertThat(event.get("attributes/meta.version", Integer.class), is(1));
        assertThat(event.get("attributes/meta.region", String.class), is("us-east-1"));
    }

    @Test
    void testTargetConflictStrategy_flattenedKeysWithSlashAndTilde_whenTargetExists() {
        when(mockConfig.getSource()).thenReturn("nested");
        when(mockConfig.getTarget()).thenReturn("attributes");
        when(mockConfig.getTargetConflictStrategy()).thenReturn(FieldConflictStrategy.MERGE_PRESERVE_EXISTING_KEYS);

        final FlattenProcessor processor = createObjectUnderTest();

        final Map<String, Object> existingAttributes = new HashMap<>();
        existingAttributes.put("existing", "keep");

        final Map<String, Object> nested = Map.of("user/name", "Reshmi", "key~name", "val");

        final Map<String, Object> initialData = new HashMap<>();
        initialData.put("attributes", existingAttributes);
        initialData.put("nested", nested);

        final Record<Event> record = createTestRecord(initialData);
        final List<Record<Event>> results = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));
        final Event event = results.get(0).getData();

        assertThat(event.get("attributes/existing", String.class), is("keep"));
        // mergeFields interprets keys as JSON Pointer paths where '/' traverses nested segments
        assertThat(event.get("attributes/user/name", String.class), is("Reshmi"));
        assertThat(event.get("attributes/key~0name", String.class), is("val"));
    }

    @Test
    void testTargetConflictStrategy_flattenedKeysWithSlashAndTilde_whenTargetAbsent() {
        when(mockConfig.getSource()).thenReturn("nested");
        when(mockConfig.getTarget()).thenReturn("attributes");
        when(mockConfig.getTargetConflictStrategy()).thenReturn(FieldConflictStrategy.MERGE_PRESERVE_EXISTING_KEYS);

        final FlattenProcessor processor = createObjectUnderTest();

        final Map<String, Object> nested = Map.of("user/name", "Reshmi", "key~name", "val");

        final Map<String, Object> initialData = new HashMap<>();
        initialData.put("nested", nested);

        final Record<Event> record = createTestRecord(initialData);
        final List<Record<Event>> results = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));
        final Event event = results.get(0).getData();

        // When target is absent, JacksonEvent puts the map directly so keys are literal map entries
        assertThat(event.get("attributes/user~1name", String.class), is("Reshmi"));
        assertThat(event.get("attributes/key~0name", String.class), is("val"));
    }

    @Test
    void testTargetConflictStrategy_flattenSeparatorDot_preservesLiteralKeysInTargetMap() {
        when(mockConfig.getSource()).thenReturn("nested");
        when(mockConfig.getTarget()).thenReturn("attributes");
        when(mockConfig.getTargetConflictStrategy()).thenReturn(FieldConflictStrategy.MERGE_PRESERVE_EXISTING_KEYS);

        final FlattenProcessor processor = createObjectUnderTest();

        final Map<String, Object> existingAttributes = new HashMap<>();
        existingAttributes.put("existing_key", "existing_value");

        final Map<String, Object> nested = Map.of("parent", Map.of("child", "nested_val"));

        final Map<String, Object> initialData = new HashMap<>();
        initialData.put("attributes", existingAttributes);
        initialData.put("nested", nested);

        final Record<Event> record = createTestRecord(initialData);
        final List<Record<Event>> results = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));
        final Event event = results.get(0).getData();

        assertThat(event.get("attributes/existing_key", String.class), is("existing_value"));
        assertThat(event.get("attributes/parent.child", String.class), is("nested_val"));

        @SuppressWarnings("unchecked")
        final Map<String, Object> attributesMap = event.get("attributes", Map.class);
        assertThat(attributesMap.containsKey("existing_key"), is(true));
        assertThat(attributesMap.containsKey("parent.child"), is(true));
        assertThat(attributesMap.get("parent.child"), is("nested_val"));
    }

    @Test
    void testTargetConflictStrategy_nonMapTarget_mergePreserveSkips() {
        when(mockConfig.getSource()).thenReturn("kv_parsed");
        when(mockConfig.getTarget()).thenReturn("attributes");
        when(mockConfig.getTargetConflictStrategy()).thenReturn(FieldConflictStrategy.MERGE_PRESERVE_EXISTING_KEYS);

        final FlattenProcessor processor = createObjectUnderTest();

        final Map<String, Object> initialData = new HashMap<>();
        initialData.put("attributes", "plain_string");
        initialData.put("kv_parsed", Map.of("logtype", "ws:access"));

        final Record<Event> record = createTestRecord(initialData);
        final List<Record<Event>> results = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));
        final Event event = results.get(0).getData();

        assertThat(event.get("attributes", String.class), is("plain_string"));
    }

    @Test
    void testTargetConflictStrategy_nonMapTarget_mergeOverwriteReplaces() {
        when(mockConfig.getSource()).thenReturn("kv_parsed");
        when(mockConfig.getTarget()).thenReturn("attributes");
        when(mockConfig.getTargetConflictStrategy()).thenReturn(FieldConflictStrategy.MERGE_OVERWRITE_EXISTING_KEYS);

        final FlattenProcessor processor = createObjectUnderTest();

        final Map<String, Object> initialData = new HashMap<>();
        initialData.put("attributes", "plain_string");
        initialData.put("kv_parsed", Map.of("logtype", "ws:access"));

        final Record<Event> record = createTestRecord(initialData);
        final List<Record<Event>> results = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));
        final Event event = results.get(0).getData();

        assertThat(event.get("attributes/logtype", String.class), is("ws:access"));
    }

    @Test
    void testTargetConflictStrategy_nullInConfigDefaultsToOverwrite() {
        when(mockConfig.getSource()).thenReturn("kv_parsed");
        when(mockConfig.getTarget()).thenReturn("attributes");
        when(mockConfig.getTargetConflictStrategy()).thenReturn(null);

        final FlattenProcessor processor = createObjectUnderTest();

        final Map<String, Object> existingAttributes = new HashMap<>();
        existingAttributes.put("cluster_name", "my-cluster");

        final Map<String, Object> kvParsed = Map.of("logtype", "ws:access");

        final Map<String, Object> initialData = new HashMap<>();
        initialData.put("attributes", existingAttributes);
        initialData.put("kv_parsed", kvParsed);

        final Record<Event> record = createTestRecord(initialData);
        final List<Record<Event>> results = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));
        final Event event = results.get(0).getData();

        assertThat(event.get("attributes/logtype", Object.class), is("ws:access"));
        assertThat(event.containsKey("attributes/cluster_name"), is(false));
    }
}