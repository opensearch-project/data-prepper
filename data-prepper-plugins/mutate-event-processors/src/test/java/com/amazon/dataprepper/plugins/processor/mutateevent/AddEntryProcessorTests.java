/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.mutateevent;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.JacksonEvent;
import com.amazon.dataprepper.model.record.Record;
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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class AddEntryProcessorTests {
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private AddEntryProcessorConfig mockConfig;

    @Test
    public void testSingleAddProcessorTests() {
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("newMessage", 3, false)));

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
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("newMessage", 3, false),
                createEntry("message2", 4, false)));

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
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("newMessage", 3, false)));

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
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("newMessage", 3, true)));

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
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("newMessage", 3, true),
                (createEntry("message", 4, false))));

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
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("newMessage", 3, false)));

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
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("newMessage", true, false)));

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
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("newMessage", "string", false)));

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
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("newMessage", null, false)));

        final AddEntryProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo("thisisamessage"));
        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
        assertThat(editedRecords.get(0).getData().get("newMessage", Object.class), equalTo(null));
    }

    @Test
    public void testFloatAddProcessorTests() {
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("newMessage", 1.2, false)));

        final AddEntryProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo("thisisamessage"));
        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
        assertThat(editedRecords.get(0).getData().get("newMessage", Object.class), equalTo(1.2));
    }

    private AddEntryProcessor createObjectUnderTest() {
        return new AddEntryProcessor(pluginMetrics, mockConfig);
    }

    private AddEntryProcessorConfig.Entry createEntry(final String key, final Object value, final boolean overwriteIfKeyExists) {
        return new AddEntryProcessorConfig.Entry(key, value, overwriteIfKeyExists);
    }

    private List<AddEntryProcessorConfig.Entry> createListOfEntries(final AddEntryProcessorConfig.Entry... entries) {
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
