/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutatestring;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.event.TestEventFactory;
import org.opensearch.dataprepper.event.TestEventKeyFactory;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventBuilder;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.event.EventKeyFactory;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class UppercaseStringProcessorTests {
    private static final EventFactory TEST_EVENT_FACTORY = TestEventFactory.getTestEventFactory();
    private final EventKeyFactory eventKeyFactory = TestEventKeyFactory.getTestEventFactory();

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private WithKeysConfig config;

    @BeforeEach
    public void setup() {
        lenient().when(config.getIterativeConfig()).thenReturn(Stream.of("message").map(eventKeyFactory::createEventKey).collect(Collectors.toList()));
    }

    @Test
    public void testHappyPathUppercaseStringProcessor() {
        final UppercaseStringProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo("THISISAMESSAGE"));
    }

    @Test
    public void testHappyPathMultiUppercaseStringProcessor() {
        when(config.getIterativeConfig()).thenReturn(Stream.of("message", "message2").map(eventKeyFactory::createEventKey).collect(Collectors.toList()));

        final UppercaseStringProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        record.getData().put("message2", "test2");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo("THISISAMESSAGE"));
        assertThat(editedRecords.get(0).getData().containsKey("message2"), is(true));
        assertThat(editedRecords.get(0).getData().get("message2", Object.class), equalTo("TEST2"));
    }

    @Test
    public void testHappyPathMultiMixedUppercaseStringProcessor() {
        lenient().when(config.getIterativeConfig()).thenReturn(Stream.of("message", "message2").map(eventKeyFactory::createEventKey).collect(Collectors.toList()));

        final UppercaseStringProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        record.getData().put("message2", 3);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo("THISISAMESSAGE"));
        assertThat(editedRecords.get(0).getData().containsKey("message2"), is(true));
        assertThat(editedRecords.get(0).getData().get("message2", Object.class), equalTo(3));
    }

    @Test
    public void testValueIsNotStringUppercaseStringProcessor() {
        final UppercaseStringProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent(3);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo(3));
    }

    @Test
    public void testValueIsNullUppercaseStringProcessor() {
        final UppercaseStringProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent(null);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo(null));
    }

    @Test
    public void testValueIsObjectUppercaseStringProcessor() {
        final UppercaseStringProcessor processor = createObjectUnderTest();
        final TestObject testObject = new TestObject();
        testObject.a = "msg";
        final Record<Event> record = getEvent(testObject);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", TestObject.class), equalTo(testObject));
        assertThat(editedRecords.get(0).getData().get("message", TestObject.class).a, equalTo(testObject.a));
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

    private UppercaseStringProcessor createObjectUnderTest() {
        return new UppercaseStringProcessor(pluginMetrics, config);
    }

    private Record<Event> getEvent(Object message) {
        final Map<String, Object> testData = new HashMap();
        testData.put("message", message);
        return buildRecordWithEvent(testData);
    }

    private static Record<Event> buildRecordWithEvent(final Map<String, Object> data) {
        return new Record<>(TEST_EVENT_FACTORY.eventBuilder(EventBuilder.class)
                .withData(data)
                .withEventType("event")
                .build());
    }
}
