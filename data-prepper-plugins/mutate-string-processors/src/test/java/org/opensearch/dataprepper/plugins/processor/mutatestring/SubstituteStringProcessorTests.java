/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutatestring;

import org.opensearch.dataprepper.event.TestEventFactory;
import org.opensearch.dataprepper.event.TestEventKeyFactory;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventBuilder;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyFactory;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.record.Record;
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
import java.util.UUID;
import org.opensearch.dataprepper.model.pattern.PatternSyntaxException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class SubstituteStringProcessorTests {
    private static final EventFactory TEST_EVENT_FACTORY = TestEventFactory.getTestEventFactory();
    private final EventKeyFactory eventKeyFactory = TestEventKeyFactory.getTestEventFactory();
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private SubstituteStringProcessorConfig config;

    @Mock
    private ExpressionEvaluator expressionEvaluator;


    @BeforeEach
    public void setup() {
        lenient().when(config.getIterativeConfig()).thenReturn(Collections.singletonList(createEntry("message", "a", "b", null)));
        lenient().when(config.getEntries()).thenReturn(Collections.singletonList(createEntry("message", "a", "b", null)));
    }

    @Test
    void invalid_substitute_when_throws_InvalidPluginConfigurationException() {
        final String substituteWhen = UUID.randomUUID().toString();
        when(config.getEntries()).thenReturn(Collections.singletonList(createEntry("message", "a", "b", substituteWhen)));

        when(expressionEvaluator.isValidExpressionStatement(substituteWhen)).thenReturn(false);

        assertThrows(InvalidPluginConfigurationException.class, this::createObjectUnderTest);
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
    public void testNoMatchSubstituteStringProcessor() {
        final SubstituteStringProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("qwerty");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo("qwerty"));
    }

    @Test
    public void testHappyPathMultiSubstituteStringProcessor() {
        when(config.getIterativeConfig()).thenReturn(Arrays.asList(createEntry("message", "a", "b", null),
                createEntry("message2", "c", "d", null)));
        when(config.getEntries()).thenReturn(Arrays.asList(createEntry("message", "a", "b", null),
                createEntry("message2", "c", "d", null)));

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
        when(config.getIterativeConfig()).thenReturn(Arrays.asList(createEntry("message", "[?\\\\+]", "b", null),
                createEntry("message2", "c", "d", null)));
        when(config.getEntries()).thenReturn(Arrays.asList(createEntry("message", "[?\\\\+]", "b", null),
                createEntry("message2", "c", "d", null)));

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
    public void test_events_are_identical_when_substituteWhen_condition_returns_false() {
        final String substituteWhen = UUID.randomUUID().toString();

        when(config.getIterativeConfig()).thenReturn(Collections.singletonList(createEntry("message", "[?\\\\+]", "b", substituteWhen)));
        when(config.getEntries()).thenReturn(Collections.singletonList(createEntry("message", "[?\\\\+]", "b", substituteWhen)));
        when(expressionEvaluator.isValidExpressionStatement(substituteWhen)).thenReturn(true);

        final SubstituteStringProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("abcd");

        when(expressionEvaluator.evaluateConditional(substituteWhen, record.getData())).thenReturn(false);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().toMap(), equalTo(record.getData().toMap()));
    }

    @Test
    public void testShutdown() {
        final SubstituteStringProcessor processor = createObjectUnderTest();
        assertThat(processor.isReadyForShutdown(), is(true));
    }

    @Test
    void testBothKeyValuesDefinedErrorKeyValueProcessor() {
        lenient().when(config.getIterativeConfig()).thenReturn(Collections.singletonList(createEntry("message", "[", "b", null)));
        lenient().when(config.getEntries()).thenReturn(Collections.singletonList(createEntry("message", "[", "b", null)));

        assertThrows(PatternSyntaxException.class, () -> new SubstituteStringProcessor(pluginMetrics, config, expressionEvaluator));
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

    private SubstituteStringProcessorConfig.Entry createEntry(final String source, final String from, final String to, final String substituteWhen) {
        final EventKey sourceKey = eventKeyFactory.createEventKey(source);
        final SubstituteStringProcessorConfig.Entry entry = new SubstituteStringProcessorConfig.Entry(sourceKey, from, to, substituteWhen);

        return entry;
    }

    private SubstituteStringProcessor createObjectUnderTest() {
        return new SubstituteStringProcessor(pluginMetrics, config, expressionEvaluator);
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
