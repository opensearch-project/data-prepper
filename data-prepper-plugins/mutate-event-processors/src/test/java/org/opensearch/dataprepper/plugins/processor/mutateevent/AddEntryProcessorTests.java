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
public class AddEntryProcessorTests {
    private static final String TEST_FORMAT = "${date} ${time}";
    private static final String ANOTHER_TEST_FORMAT = "${date}T${time}";
    private static final String BAD_TEST_FORMAT = "${date} ${time";

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private AddEntryProcessorConfig mockConfig;

    @Mock
    private ExpressionEvaluator<Boolean> expressionEvaluator;

    @Test
    public void testSingleAddProcessorTests() {
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("newMessage", 3, null, false, null)));

        final AddEntryProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo("thisisamessage"));
        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
        assertThat(editedRecords.get(0).getData().get("newMessage", Object.class), equalTo(3));
    }

    @Test
    public void testMultiAddProcessorTests() {
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("newMessage", 3, null, false, null),
                createEntry("message2", 4, null, false, null)));

        final AddEntryProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo("thisisamessage"));
        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
        assertThat(editedRecords.get(0).getData().get("newMessage", Object.class), equalTo(3));
        assertThat(editedRecords.get(0).getData().containsKey("message2"), is(true));
        assertThat(editedRecords.get(0).getData().get("message2", Object.class), equalTo(4));
    }

    @Test
    public void testSingleNoOverwriteAddProcessorTests() {
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("newMessage", 3, null, false, null)));

        final AddEntryProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        record.getData().put("newMessage", "test");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo("thisisamessage"));
        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
        assertThat(editedRecords.get(0).getData().get("newMessage", Object.class), equalTo("test"));
    }

    @Test
    public void testSingleOverwriteAddProcessorTests() {
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("newMessage", 3, null, true, null)));

        final AddEntryProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        record.getData().put("newMessage", "test");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo("thisisamessage"));
        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
        assertThat(editedRecords.get(0).getData().get("newMessage", Object.class), equalTo(3));
    }

    @Test
    public void testMultiOverwriteMixedAddProcessorTests() {
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("newMessage", 3, null, true, null),
                createEntry("message", 4, null, false, null)));

        final AddEntryProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        record.getData().put("newMessage", "test");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
        assertThat(editedRecords.get(0).getData().get("newMessage", Object.class), equalTo(3));
        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo("thisisamessage"));
    }

    @Test
    public void testIntAddProcessorTests() {
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("newMessage", 3, null, false, null)));

        final AddEntryProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo("thisisamessage"));
        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
        assertThat(editedRecords.get(0).getData().get("newMessage", Object.class), equalTo(3));
    }

    @Test
    public void testBoolAddProcessorTests() {
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("newMessage", true, null, false, null)));

        final AddEntryProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo("thisisamessage"));
        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
        assertThat(editedRecords.get(0).getData().get("newMessage", Object.class), equalTo(true));
    }

    @Test
    public void testStringAddProcessorTests() {
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("newMessage", "string", null, false, null)));

        final AddEntryProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo("thisisamessage"));
        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
        assertThat(editedRecords.get(0).getData().get("newMessage", Object.class), equalTo("string"));
    }

    @Test
    public void testNullAddProcessorTests() {
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("newMessage", null, null, false, null)));

        final AddEntryProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo("thisisamessage"));
        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
        assertThat(editedRecords.get(0).getData().get("newMessage", Object.class), equalTo(null));
    }

    private static class TestObject {
        public String a;

        @Override
        public boolean equals(Object o) {
            TestObject testObject = (TestObject) o;
            return this.a == testObject.a;
        }
    }

    @Test
    public void testNestedAddProcessorTests() {
        TestObject obj = new TestObject();
        obj.a = "test";
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("newMessage", obj, null, false, null)));

        final AddEntryProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo("thisisamessage"));
        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
        assertThat(editedRecords.get(0).getData().get("newMessage", TestObject.class), equalTo(obj));
    }

    @Test
    public void testArrayAddProcessorTests() {
        Object[] array = new Object[] { 1, 1.2, "string", true, null };
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("newMessage", array, null, false, null)));

        final AddEntryProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo("thisisamessage"));
        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
        assertThat(editedRecords.get(0).getData().get("newMessage", Object[].class), equalTo(array));
    }

    @Test
    public void testFloatAddProcessorTests() {
        when(mockConfig.getEntries())
                .thenReturn(createListOfEntries(createEntry("newMessage", 1.2, null, false, null)));

        final AddEntryProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo("thisisamessage"));
        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
        assertThat(editedRecords.get(0).getData().get("newMessage", Object.class), equalTo(1.2));
    }

    @Test
    public void testAddSingleFormatEntry() {
        when(mockConfig.getEntries())
                .thenReturn(createListOfEntries(createEntry("date-time", null, TEST_FORMAT, false, null)));

        final AddEntryProcessor processor = createObjectUnderTest();
        final Record<Event> record = getTestEventWithMultipleFields();
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        Event event = editedRecords.get(0).getData();
        assertThat(event.get("date", Object.class), equalTo("date-value"));
        assertThat(event.get("time", Object.class), equalTo("time-value"));
        assertThat(event.get("date-time", Object.class), equalTo("date-value time-value"));
    }

    @Test
    public void testAddMultipleFormatEntries() {
        when(mockConfig.getEntries())
                .thenReturn(createListOfEntries(createEntry("date-time", null, TEST_FORMAT, false, null),
                        createEntry("date-time2", null, ANOTHER_TEST_FORMAT, false, null)));

        final AddEntryProcessor processor = createObjectUnderTest();
        final Record<Event> record = getTestEventWithMultipleFields();
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        Event event = editedRecords.get(0).getData();
        assertThat(event.get("date", Object.class), equalTo("date-value"));
        assertThat(event.get("time", Object.class), equalTo("time-value"));
        assertThat(event.get("date-time", Object.class), equalTo("date-value time-value"));
        assertThat(event.get("date-time2", Object.class), equalTo("date-valueTtime-value"));
    }

    @Test
    public void testFormatOverwritesExistingEntry() {
        when(mockConfig.getEntries())
                .thenReturn(
                        createListOfEntries(createEntry("time", null, TEST_FORMAT, true, null)));

        final AddEntryProcessor processor = createObjectUnderTest();
        final Record<Event> record = getTestEventWithMultipleFields();
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        Event event = editedRecords.get(0).getData();
        assertThat(event.get("date", Object.class), equalTo("date-value"));
        assertThat(event.get("time", Object.class), equalTo("date-value time-value"));
    }

    @Test
    public void testFormatNotOverwriteExistingEntry() {
        when(mockConfig.getEntries())
                .thenReturn(
                        createListOfEntries(createEntry("time", null, TEST_FORMAT, false, null)));

        final AddEntryProcessor processor = createObjectUnderTest();
        final Record<Event> record = getTestEventWithMultipleFields();
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        Event event = editedRecords.get(0).getData();
        assertThat(event.get("date", Object.class), equalTo("date-value"));
        assertThat(event.get("time", Object.class), equalTo("time-value"));
        assertThat(event.containsKey("date-time"), equalTo(false));
    }

    @Test
    public void testFormatPrecedesValue() {
        when(mockConfig.getEntries())
                .thenReturn(
                        createListOfEntries(createEntry("date-time", "date-time-value", TEST_FORMAT, false, null)));

        final AddEntryProcessor processor = createObjectUnderTest();
        final Record<Event> record = getTestEventWithMultipleFields();
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        Event event = editedRecords.get(0).getData();
        assertThat(event.get("date", Object.class), equalTo("date-value"));
        assertThat(event.get("time", Object.class), equalTo("time-value"));
        assertThat(event.get("date-time", Object.class), equalTo("date-value time-value"));
    }

    @Test
    public void testFormatVariousDataTypes() {
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry(
                "newField", null, "${number-key}-${boolean-key}-${string-key}", false, null)));

        final AddEntryProcessor processor = createObjectUnderTest();
        final Record<Event> record = getTestEventWithMultipleDataTypes();
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        Event event = editedRecords.get(0).getData();
        assertThat(event.get("newField", Object.class), equalTo("1-true-string-value"));
    }

    @Test
    public void testBadFormatThenEntryNotAdded() {
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("data-time", null, BAD_TEST_FORMAT, false, null)));

        final AddEntryProcessor processor = createObjectUnderTest();
        final Record<Event> record = getTestEventWithMultipleFields();
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        Event event = editedRecords.get(0).getData();
        assertThat(event.get("date", Object.class), equalTo("date-value"));
        assertThat(event.get("time", Object.class), equalTo("time-value"));
        assertThat(event.containsKey("data-time"), equalTo(false));
    }

    @Test
    public void testKeyIsNotAdded_when_addWhen_condition_is_false() {
        final String addWhen = UUID.randomUUID().toString();

        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("newMessage", 3, null, false, addWhen)));

        final AddEntryProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");

        when(expressionEvaluator.evaluate(addWhen, record.getData())).thenReturn(false);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo("thisisamessage"));
        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(false));
    }

    private AddEntryProcessor createObjectUnderTest() {
        return new AddEntryProcessor(pluginMetrics, mockConfig, expressionEvaluator);
    }

    private AddEntryProcessorConfig.Entry createEntry(
            final String key, final Object value, final String format, final boolean overwriteIfKeyExists, final String addWhen) {
        return new AddEntryProcessorConfig.Entry(key, value, format, overwriteIfKeyExists, addWhen);
    }

    private List<AddEntryProcessorConfig.Entry> createListOfEntries(final AddEntryProcessorConfig.Entry... entries) {
        return new LinkedList<>(Arrays.asList(entries));
    }

    private Record<Event> getEvent(String message) {
        final Map<String, Object> testData = new HashMap<>();
        testData.put("message", message);
        return buildRecordWithEvent(testData);
    }

    private Record<Event> getTestEventWithMultipleFields() {
        Map<String, Object> data = Map.of("date", "date-value", "time", "time-value");
        return buildRecordWithEvent(data);
    }

    private Record<Event> getTestEventWithMultipleDataTypes() {
        Map<String, Object> data = Map.of(
                "number-key", 1,
                "boolean-key", true,
                "string-key", "string-value");
        return buildRecordWithEvent(data);
    }

    private static Record<Event> buildRecordWithEvent(final Map<String, Object> data) {
        return new Record<>(JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build());
    }
}
