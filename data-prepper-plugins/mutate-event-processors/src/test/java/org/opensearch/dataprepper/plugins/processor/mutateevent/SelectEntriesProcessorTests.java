/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class SelectEntriesProcessorTests {
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private SelectEntriesProcessorConfig mockConfig;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    @Test
    public void testSelectEntriesProcessorTest() {
        when(mockConfig.getIncludeKeys()).thenReturn(new String[] { "key1", "key2"});
        when(mockConfig.getSelectWhen()).thenReturn(null);
        final SelectEntriesProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        record.getData().put("newMessage", "test");
        final String value1 = UUID.randomUUID().toString();
        final String value2 = UUID.randomUUID().toString();
        record.getData().put("key1", value1);
        record.getData().put("key2", value2);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));
        assertThat(editedRecords.get(0).getData().containsKey("key1"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("key2"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(false));
        assertThat(editedRecords.get(0).getData().containsKey("message"), is(false));
        assertThat(editedRecords.get(0).getData().get("key1", String.class), equalTo(value1));
        assertThat(editedRecords.get(0).getData().get("key2", String.class), equalTo(value2));
    }

    @Test
    public void testWithKeyDneSelectEntriesProcessorTest() {
        when(mockConfig.getIncludeKeys()).thenReturn(new String[] { "key1", "key2"});
        when(mockConfig.getSelectWhen()).thenReturn(null);
        final SelectEntriesProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        record.getData().put("newMessage", "test");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));
        assertThat(editedRecords.get(0).getData().containsKey("key1"), is(false));
        assertThat(editedRecords.get(0).getData().containsKey("key2"), is(false));
        assertThat(editedRecords.get(0).getData().containsKey("message"), is(false));
        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(false));
    }

    @Test
    public void testSelectEntriesProcessorWithConditionTest() {
        when(mockConfig.getIncludeKeys()).thenReturn(new String[] { "key1", "key2"});
        final String selectWhen = UUID.randomUUID().toString();
        when(mockConfig.getSelectWhen()).thenReturn(selectWhen);
        final SelectEntriesProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        record.getData().put("newMessage", "test");
        final String value1 = UUID.randomUUID().toString();
        final String value2 = UUID.randomUUID().toString();
        record.getData().put("key1", value1);
        record.getData().put("key2", value2);
        when(expressionEvaluator.evaluateConditional(selectWhen, record.getData())).thenReturn(false);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));
        assertThat(editedRecords.get(0).getData().containsKey("key1"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("key2"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("key1", String.class), equalTo(value1));
        assertThat(editedRecords.get(0).getData().get("key2", String.class), equalTo(value2));
    }

    @Test
    public void testNestedSelectEntriesProcessorTest() {
        when(mockConfig.getIncludeKeys()).thenReturn(new String[] { "nested/key1", "nested/nested2/key2"});
        when(mockConfig.getSelectWhen()).thenReturn(null);
        final String value1 = UUID.randomUUID().toString();
        final String value2 = UUID.randomUUID().toString();
        Map<String, Object> nested2 = Map.of("key2", value2, "key3", "value3");
        Map<String, Object> nested = Map.of("key1", value1, "fizz", 42, "nested2", nested2);
        final SelectEntriesProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        record.getData().put("nested", nested);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));
        assertThat(editedRecords.get(0).getData().containsKey("nested/key1"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("nested/nested2/key2"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("nested/nested2/key3"), is(false));
        assertThat(editedRecords.get(0).getData().containsKey("nested/fizz"), is(false));
        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(false));
        assertThat(editedRecords.get(0).getData().containsKey("message"), is(false));

        assertThat(editedRecords.get(0).getData().get("nested/key1", String.class), equalTo(value1));
        assertThat(editedRecords.get(0).getData().get("nested/nested2/key2", String.class), equalTo(value2));
    }


    private SelectEntriesProcessor createObjectUnderTest() {
        return new SelectEntriesProcessor(pluginMetrics, mockConfig, expressionEvaluator);
    }

    private Record<Event> getEvent(String message) {
        final Map<String, Object> testData = new HashMap();
        testData.put("message", message);
        return buildRecordWithEvent(testData);
    }

    private static Record<Event> buildRecordWithEvent(final Map<String, Object> data) {
        return new Record<>(JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build());
    }
}
