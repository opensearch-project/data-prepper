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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CopyValueProcessorTests {
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private CopyValueProcessorConfig mockConfig;

    @Mock
    private ExpressionEvaluator<Boolean> expressionEvaluator;

    @Test
    public void testSingleCopyProcessorTests() {
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("message", "newMessage", false, null)));

        final CopyValueProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("newMessage", Object.class), equalTo("thisisamessage"));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo("thisisamessage"));
    }

    @Test
    public void testMultiCopyProcessorTests() {
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("message", "newMessage", false, null),
                createEntry("message2", "entry", false, null)));

        final CopyValueProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        record.getData().put("message2", "test");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("newMessage", Object.class), equalTo("thisisamessage"));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo("thisisamessage"));
        assertThat(editedRecords.get(0).getData().containsKey("entry"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("message2"), is(true));
        assertThat(editedRecords.get(0).getData().get("entry", Object.class), equalTo("test"));
        assertThat(editedRecords.get(0).getData().get("message2", Object.class), equalTo("test"));
    }

    @Test
    public void testNoOverwriteSingleCopyProcessorTests() {
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("message", "newMessage", false, null)));

        final CopyValueProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        record.getData().put("newMessage", "test");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("newMessage", Object.class), equalTo("test"));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo("thisisamessage"));
    }

    private static class TestObject {
        public String a;

        @Override
        public boolean equals(Object o) {
            if(o instanceof TestObject) {
                TestObject testObject = (TestObject) o;
                return this.a == testObject.a;
            }

            return false;
        }
    }

    @Test
    public void testNestedObjectCopyProcessorTests() {
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("message", "newMessage", true, null)));

        final CopyValueProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        TestObject data = new TestObject();
        data.a = "test";
        record.getData().put("message", data);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("newMessage", TestObject.class), equalTo(data));
        assertThat(editedRecords.get(0).getData().get("message", TestObject.class), equalTo(data));
    }

    @Test
    public void testFromKeyDneCopyProcessorTests() {
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("message2", "newMessage", false, null)));

        final CopyValueProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(false));
        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo("thisisamessage"));
    }

    @Test
    public void testOverwriteSingleCopyProcessorTests() {
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("message", "newMessage", true, null)));

        final CopyValueProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        record.getData().put("newMessage", "test");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("newMessage", Object.class), equalTo("thisisamessage"));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo("thisisamessage"));
    }

    @Test
    public void testOverwriteMixedSingleCopyProcessorTests() {
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("message", "newMessage", false, null),
                createEntry("message2", "entry", true, null)));

        final CopyValueProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        record.getData().put("newMessage", "test");
        record.getData().put("message2", "test2");
        record.getData().put("entry", "test3");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("message2"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("entry"), is(true));
        assertThat(editedRecords.get(0).getData().get("newMessage", Object.class), equalTo("test"));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo("thisisamessage"));
        assertThat(editedRecords.get(0).getData().get("message2", Object.class), equalTo("test2"));
        assertThat(editedRecords.get(0).getData().get("entry", Object.class), equalTo("test2"));
    }

    @Test
    public void testKey_is_not_copied_when_copyWhen_returns_false() {
        final String copyWhen = UUID.randomUUID().toString();


        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("message2", "newMessage", true, copyWhen)));

        final CopyValueProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        when(expressionEvaluator.evaluate(copyWhen, record.getData())).thenReturn(false);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(false));
        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo("thisisamessage"));
    }

    private CopyValueProcessor createObjectUnderTest() {
        return new CopyValueProcessor(pluginMetrics, mockConfig, expressionEvaluator);
    }

    private CopyValueProcessorConfig.Entry createEntry(final String fromKey, final String toKey, final boolean overwriteIfToKeyExists, final String copyWhen) {
        return new CopyValueProcessorConfig.Entry(fromKey, toKey, overwriteIfToKeyExists, copyWhen);
    }

    private List<CopyValueProcessorConfig.Entry> createListOfEntries(final CopyValueProcessorConfig.Entry... entries) {
        return new LinkedList<>(Arrays.asList(entries));
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
