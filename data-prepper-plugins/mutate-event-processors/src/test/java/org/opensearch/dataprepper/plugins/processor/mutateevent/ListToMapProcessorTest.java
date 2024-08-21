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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ListToMapProcessorTest {
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private ListToMapProcessorConfig mockConfig;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    @BeforeEach
    void setUp() {
        lenient().when(mockConfig.getTarget()).thenReturn(null);
        lenient().when(mockConfig.getValueKey()).thenReturn(null);
        lenient().when(mockConfig.getFlatten()).thenReturn(false);
        lenient().when(mockConfig.getFlattenedElement()).thenReturn(ListToMapProcessorConfig.FlattenedElement.FIRST);
        lenient().when(mockConfig.getListToMapWhen()).thenReturn(null);
        lenient().when(mockConfig.getUseSourceKey()).thenReturn(false);
        lenient().when(mockConfig.getExtractValue()).thenReturn(false);
    }

    @Test
    void invalid_list_to_map_when_throws_InvalidPluginConfigurationException() {
        final String listToMapWhen = UUID.randomUUID().toString();
        when(mockConfig.getListToMapWhen()).thenReturn(listToMapWhen);

        when(expressionEvaluator.isValidExpressionStatement(listToMapWhen)).thenReturn(false);

        assertThrows(InvalidPluginConfigurationException.class, this::createObjectUnderTest);
    }

    @Test
    public void testValueExtractionWithFlattenAndWriteToRoot() {
        when(mockConfig.getValueKey()).thenReturn("value");
        when(mockConfig.getSource()).thenReturn("mylist");
        when(mockConfig.getKey()).thenReturn("name");
        when(mockConfig.getFlatten()).thenReturn(true);

        final ListToMapProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord();
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        assertThat(resultEvent.get("a", String.class), is("val-a"));
        assertThat(resultEvent.get("b", String.class), is("val-b1"));
        assertThat(resultEvent.get("c", String.class), is("val-c"));
    }

    @Test
    public void testValueExtractionWithFlattenAndWriteToRoot_UseSourceKey() {
        when(mockConfig.getSource()).thenReturn("mylist");
        when(mockConfig.getFlatten()).thenReturn(true);
        when(mockConfig.getUseSourceKey()).thenReturn(true);
        when(mockConfig.getExtractValue()).thenReturn(true);

        final ListToMapProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord();
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        assertThat(resultEvent.get("name", String.class), is("a"));
        assertThat(resultEvent.get("value", String.class), is("val-a"));
    }

    @Test
    public void testValueExtractionWithFlattenKeepLastElementAndWriteToRoot_UseSourceKey() {
        when(mockConfig.getSource()).thenReturn("mylist");
        when(mockConfig.getFlatten()).thenReturn(true);
        when(mockConfig.getFlattenedElement()).thenReturn(ListToMapProcessorConfig.FlattenedElement.LAST);
        when(mockConfig.getUseSourceKey()).thenReturn(true);
        when(mockConfig.getExtractValue()).thenReturn(true);

        final ListToMapProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord();
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        assertThat(resultEvent.get("name", String.class), is("c"));
        assertThat(resultEvent.get("value", String.class), is("val-c"));
    }

    @Test
    public void testValueExtractionWithFlattenKeepLastElementAndWriteToRoot() {
        when(mockConfig.getValueKey()).thenReturn("value");
        when(mockConfig.getSource()).thenReturn("mylist");
        when(mockConfig.getKey()).thenReturn("name");
        when(mockConfig.getFlatten()).thenReturn(true);
        when(mockConfig.getFlattenedElement()).thenReturn(ListToMapProcessorConfig.FlattenedElement.LAST);

        final ListToMapProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord();
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        assertThat(resultEvent.get("a", String.class), is("val-a"));
        assertThat(resultEvent.get("b", String.class), is("val-b2"));
        assertThat(resultEvent.get("c", String.class), is("val-c"));
    }

    @Test
    public void testValueExtractionWithFlattenAndWriteToTarget() {
        when(mockConfig.getValueKey()).thenReturn("value");
        when(mockConfig.getSource()).thenReturn("mylist");
        when(mockConfig.getKey()).thenReturn("name");
        when(mockConfig.getTarget()).thenReturn("mymap");
        when(mockConfig.getFlatten()).thenReturn(true);

        final ListToMapProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord();
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        assertThat(resultEvent.get("mymap/a", String.class), is("val-a"));
        assertThat(resultEvent.get("mymap/b", String.class), is("val-b1"));
        assertThat(resultEvent.get("mymap/c", String.class), is("val-c"));
    }

    @Test
    public void testNoValueExtractionWithFlattenAndWriteToRoot() {
        when(mockConfig.getSource()).thenReturn("mylist");
        when(mockConfig.getKey()).thenReturn("name");
        when(mockConfig.getFlatten()).thenReturn(true);

        final ListToMapProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord();
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        assertThat(resultEvent.get("a", Object.class), is(Map.of("name", "a", "value", "val-a")));
        assertThat(resultEvent.get("b", Object.class), is(Map.of("name", "b", "value", "val-b1")));
        assertThat(resultEvent.get("c", Object.class), is(Map.of("name", "c", "value", "val-c")));
    }

    @Test
    public void testValueExtractionWithNoFlattenAndWriteToRoot() {
        when(mockConfig.getSource()).thenReturn("mylist");
        when(mockConfig.getKey()).thenReturn("name");
        when(mockConfig.getValueKey()).thenReturn("value");

        final ListToMapProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord();
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        assertThat(resultEvent.get("a", Object.class), is(List.of("val-a")));
        assertThat(resultEvent.get("b", Object.class), is(List.of("val-b1", "val-b2")));
        assertThat(resultEvent.get("c", Object.class), is(List.of("val-c")));
    }

    @Test
    public void testValueExtractionWithNoFlattenAndWriteToRoot_UseSourceKey() {
        when(mockConfig.getSource()).thenReturn("mylist");
        when(mockConfig.getUseSourceKey()).thenReturn(true);
        when(mockConfig.getExtractValue()).thenReturn(true);

        final ListToMapProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord();
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        assertThat(resultEvent.get("name", List.class), is(List.of("a", "b", "b", "c")));
        assertThat(resultEvent.get("value", List.class), is(List.of("val-a", "val-b1", "val-b2", "val-c")));
    }

    @Test
    public void testValueExtractionWithNoFlattenAndWriteToRoot_UseSourceKey_InconsistentKeysInRecord() {
        when(mockConfig.getSource()).thenReturn("mylist");
        when(mockConfig.getUseSourceKey()).thenReturn(true);
        when(mockConfig.getExtractValue()).thenReturn(true);

        final ListToMapProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecordWithInconsistentKeys();
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        assertThat(resultEvent.get("name", List.class), is(List.of("a", "b")));
        assertThat(resultEvent.get("badname", List.class), is(List.of("c")));
        assertThat(resultEvent.get("value", List.class), is(List.of("val-a", "val-b", "val-c")));
    }

    @Test
    public void testNoValueExtractionWithNoFlattenAndWriteToRoot() {
        when(mockConfig.getSource()).thenReturn("mylist");
        when(mockConfig.getKey()).thenReturn("name");

        final ListToMapProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord();
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        assertThat(resultEvent.get("a", Object.class), is(List.of(Map.of("name", "a", "value", "val-a"))));
        assertThat(resultEvent.get("b", Object.class), is(List.of(
                Map.of("name", "b", "value", "val-b1"),
                Map.of("name", "b", "value", "val-b2")
        )));
        assertThat(resultEvent.get("c", Object.class), is(List.of(Map.of("name", "c", "value", "val-c"))));
    }

    @Test
    public void testNoValueExtractionWithNoFlattenAndWriteToRoot_UseSourceKey() {
        when(mockConfig.getSource()).thenReturn("mylist");
        when(mockConfig.getUseSourceKey()).thenReturn(true);

        final ListToMapProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord();
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        assertThat(resultEvent.get("name", List.class), is(List.of(
                Map.of("name", "a", "value", "val-a"),
                Map.of("name", "b", "value", "val-b1"),
                Map.of("name", "b", "value", "val-b2"),
                Map.of("name", "c", "value", "val-c"))));
        assertThat(resultEvent.get("value", List.class), is(List.of(
                Map.of("name", "a", "value", "val-a"),
                Map.of("name", "b", "value", "val-b1"),
                Map.of("name", "b", "value", "val-b2"),
                Map.of("name", "c", "value", "val-c"))));
    }

    @Test
    public void testFailureDueToInvalidSourceKey() {
        when(mockConfig.getSource()).thenReturn("invalid_source_key");

        final ListToMapProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord();
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        assertThat(resultEvent.get("mymap", Object.class), is(nullValue()));
    }

    @Test
    public void testFailureDueToSourceNotList() {
        when(mockConfig.getSource()).thenReturn("nolist");

        final ListToMapProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord();
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        assertThat(resultEvent.get("mymap", Object.class), is(nullValue()));
    }

    @Test
    public void testFailureDueToBadEventData() {
        when(mockConfig.getValueKey()).thenReturn("value");
        when(mockConfig.getSource()).thenReturn("mylist");
        when(mockConfig.getKey()).thenReturn("name");

        final ListToMapProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecordWithInconsistentKeys();
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        assertThat(resultEvent.get("mymap", Object.class), is(nullValue()));
    }

    @Test
    public void testTagsAreAddedOnFailure() {
        when(mockConfig.getValueKey()).thenReturn("value");
        when(mockConfig.getSource()).thenReturn("mylist");
        when(mockConfig.getKey()).thenReturn("name");
        final List<String> testTags = List.of("tag1", "tag2");
        when(mockConfig.getTagsOnFailure()).thenReturn(testTags);

        final ListToMapProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecordWithInconsistentKeys();
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        assertThat(resultEvent.get("mymap", Object.class), is(nullValue()));
        assertThat(resultEvent.getMetadata().getTags(), is(new HashSet<>(testTags)));
    }

    @Test
    public void testNoValueExtraction_when_the_when_condition_returns_false() {
        final String whenCondition = UUID.randomUUID().toString();
        when(mockConfig.getListToMapWhen()).thenReturn(whenCondition);
        when(expressionEvaluator.isValidExpressionStatement(whenCondition)).thenReturn(true);

        final ListToMapProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord();

        when(expressionEvaluator.evaluateConditional(whenCondition, testRecord.getData())).thenReturn(false);
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        assertThat(resultEvent.toMap(), equalTo(testRecord.getData().toMap()));
    }

    private ListToMapProcessor createObjectUnderTest() {
        return new ListToMapProcessor(pluginMetrics, mockConfig, expressionEvaluator);
    }

    private Record<Event> createTestRecord() {
        final Map<String, Object> data = Map.of("mylist", List.of(
                Map.of("name", "a", "value", "val-a"),
                Map.of("name", "b", "value", "val-b1"),
                Map.of("name", "b", "value", "val-b2"),
                Map.of("name", "c", "value", "val-c")),
                "nolist", "single-value");
        final Event event = JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build();
        return new Record<>(event);
    }

    private Record<Event> createTestRecordWithInconsistentKeys() {
        final Map<String, Object> data = Map.of("mylist", List.of(
                        Map.of("name", "a", "value", "val-a"),
                        Map.of("name", "b", "value", "val-b"),
                        Map.of("badname", "c", "value", "val-c")));
        final Event event = JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build();
        return new Record<>(event);
    }
}
