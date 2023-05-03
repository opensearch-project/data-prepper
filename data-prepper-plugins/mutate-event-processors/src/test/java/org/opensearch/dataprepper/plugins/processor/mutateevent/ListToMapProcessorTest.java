/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ListToMapProcessorTest {
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private ListToMapProcessorConfig mockConfig;

    @Mock
    private ExpressionEvaluator<Boolean> expressionEvaluator;

    @Test
    public void testValueExtractionWithFlattenAndWriteToRoot() {
        when(mockConfig.getValueKey()).thenReturn("value");
        when(mockConfig.getSource()).thenReturn("mylist");
        when(mockConfig.getKey()).thenReturn("name");
        when(mockConfig.getFlatten()).thenReturn(true);
        when(mockConfig.getFlattenedElement()).thenReturn(ListToMapProcessorConfig.FlattenedElement.FIRST);
        when(mockConfig.getListToMapWhen()).thenReturn(null);

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
    public void testValueExtractionWithFlattenKeepLastElementAndWriteToRoot() {
        when(mockConfig.getValueKey()).thenReturn("value");
        when(mockConfig.getSource()).thenReturn("mylist");
        when(mockConfig.getKey()).thenReturn("name");
        when(mockConfig.getFlatten()).thenReturn(true);
        when(mockConfig.getFlattenedElement()).thenReturn(ListToMapProcessorConfig.FlattenedElement.LAST);
        when(mockConfig.getListToMapWhen()).thenReturn(null);

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
        when(mockConfig.getFlattenedElement()).thenReturn(ListToMapProcessorConfig.FlattenedElement.FIRST);
        when(mockConfig.getListToMapWhen()).thenReturn(null);

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
        when(mockConfig.getFlattenedElement()).thenReturn(ListToMapProcessorConfig.FlattenedElement.FIRST);
        when(mockConfig.getListToMapWhen()).thenReturn(null);

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
        when(mockConfig.getListToMapWhen()).thenReturn(null);

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
    public void testNoValueExtractionWithNoFlattenAndWriteToRoot() {
        when(mockConfig.getSource()).thenReturn("mylist");
        when(mockConfig.getKey()).thenReturn("name");
        when(mockConfig.getListToMapWhen()).thenReturn(null);

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
    public void testFailureDueToInvalidSourceKey() {
        when(mockConfig.getSource()).thenReturn("invalid_source_key");
        when(mockConfig.getListToMapWhen()).thenReturn(null);

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
        when(mockConfig.getListToMapWhen()).thenReturn(null);

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
        when(mockConfig.getListToMapWhen()).thenReturn(null);

        final ListToMapProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createBadTestRecord();
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        assertThat(resultEvent.get("mymap", Object.class), is(nullValue()));
    }

    @Test
    public void testNoValueExtraction_when_the_when_condition_returns_false() {
        final String whenCondition = UUID.randomUUID().toString();
        when(mockConfig.getListToMapWhen()).thenReturn(whenCondition);

        final ListToMapProcessor processor = createObjectUnderTest();
        final Record<Event> testRecord = createTestRecord();

        when(expressionEvaluator.evaluate(whenCondition, testRecord.getData())).thenReturn(false);
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

    private Record<Event> createBadTestRecord() {
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