/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.flatten;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
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
        lenient().when(mockConfig.getFlattenWhen()).thenReturn(null);
        lenient().when(mockConfig.getTagsOnFailure()).thenReturn(new ArrayList<>());
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
    public void testEventNotProcessedWhenTheWhenConditionIsFalse() {
        final String whenCondition = UUID.randomUUID().toString();
        when(mockConfig.getFlattenWhen()).thenReturn(whenCondition);

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
}