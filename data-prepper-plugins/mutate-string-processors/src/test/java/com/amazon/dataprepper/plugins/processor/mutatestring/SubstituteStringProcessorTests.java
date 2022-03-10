/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.mutatestring;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.JacksonEvent;
import com.amazon.dataprepper.model.record.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class SubstituteStringProcessorTests {
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private SubstituteStringProcessorConfig config;

    @BeforeEach
    public void setup() {
        lenient().when(config.getIterativeConfig()).thenReturn(Collections.singletonList(createEntry("message", "a", "b")));
        lenient().when(config.getEntries()).thenReturn(Collections.singletonList(createEntry("message", "a", "b")));
    }

    @Test
    public void testHappyPathSubstituteStringProcessor() {
        final SubstituteStringProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("abcd");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo("bbcd"));
    }

    @Test
    public void testHappyPathMultiSubstituteStringProcessor() {
        when(config.getIterativeConfig()).thenReturn(Arrays.asList(createEntry("message", "a", "b"),
                createEntry("message2", "c", "d")));
        when(config.getEntries()).thenReturn(Arrays.asList(createEntry("message", "a", "b"),
                createEntry("message2", "c", "d")));

        final SubstituteStringProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("abcd");
        record.getData().put("message2", "cdef");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo("bbcd"));
        assertThat(editedRecords.get(0).getData().containsKey("message2"), is(true));
        assertThat(editedRecords.get(0).getData().get("message2", Object.class), equalTo("ddef"));
    }

    @Test
    public void testHappyPathMultiMixedSubstituteStringProcessor() {
        when(config.getIterativeConfig()).thenReturn(Arrays.asList(createEntry("message", "[?\\\\+]", "b"),
                createEntry("message2", "c", "d")));
        when(config.getEntries()).thenReturn(Arrays.asList(createEntry("message", "[?\\\\+]", "b"),
                createEntry("message2", "c", "d")));

        final SubstituteStringProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("this?is\\a+message");
        record.getData().put("message2", 3);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo("thisbisbabmessage"));
        assertThat(editedRecords.get(0).getData().containsKey("message2"), is(true));
        assertThat(editedRecords.get(0).getData().get("message2", Object.class), equalTo(3));
    }

    @Test
    public void testValueIsNotStringSubstituteStringProcessor() {
        final SubstituteStringProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent(3);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo(3));
    }

    @Test
    public void testValueIsNullSubstituteStringProcessor() {
        final SubstituteStringProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent(null);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo(null));
    }

    @Test
    public void testValueIsObjectSubstituteStringProcessor() {
        final SubstituteStringProcessor processor = createObjectUnderTest();
        final TestObject testObject = new TestObject();
        testObject.a = "msg";
        final Record<Event> record = getEvent(testObject);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", TestObject.class), equalTo(testObject));
        assertThat(editedRecords.get(0).getData().get("message", TestObject.class).a, equalTo(testObject.a));
    }

    @Test
    public void testShutdown() {
        final SubstituteStringProcessor processor = createObjectUnderTest();
        assertThat(processor.isReadyForShutdown(), is(true));
    }

    private static class TestObject {
        public String a;

        @Override
        public boolean equals(Object other) {
            if(other instanceof TestObject) {
                return ((TestObject) other).a.equals(this.a);
            }

            return false;
        }
    }

    private SubstituteStringProcessorConfig.Entry createEntry(final String source, final String from, final String to) {
        SubstituteStringProcessorConfig.Entry entry = new SubstituteStringProcessorConfig.Entry();
        entry.source = source;
        entry.from = from;
        entry.to = to;

        return entry;
    }

    private SubstituteStringProcessor createObjectUnderTest() {
        return new SubstituteStringProcessor(pluginMetrics, config);
    }

    private Record<Event> getEvent(Object message) {
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
