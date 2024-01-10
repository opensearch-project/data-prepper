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
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
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

    private Record<Event> createBadTestRecord() {
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
}
