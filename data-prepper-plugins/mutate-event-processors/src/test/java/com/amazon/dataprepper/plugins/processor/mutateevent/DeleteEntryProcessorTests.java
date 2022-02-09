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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DeleteEntryProcessorTests {
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private DeleteEntryProcessorConfig mockConfig;

    @Test
    public void testSingleDeleteProcessorTest() {
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("message")));

        final DeleteEntryProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        record.getData().put("newMessage", "test");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(false));
        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
    }

    @Test
    public void testMultiDeleteProcessorTest() {
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("message"),
                createEntry("message2")));

        final DeleteEntryProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        record.getData().put("message2", "test");
        record.getData().put("newMessage", "test");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(false));
        assertThat(editedRecords.get(0).getData().containsKey("message2"), is(false));
        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
    }

    private DeleteEntryProcessor createObjectUnderTest() {
        return new DeleteEntryProcessor(pluginMetrics, mockConfig);
    }

    private DeleteEntryProcessorConfig.Entry createEntry(final String withKey) {
        return new DeleteEntryProcessorConfig.Entry(withKey);
    }

    private List<DeleteEntryProcessorConfig.Entry> createListOfEntries(final DeleteEntryProcessorConfig.Entry... entries) {
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
