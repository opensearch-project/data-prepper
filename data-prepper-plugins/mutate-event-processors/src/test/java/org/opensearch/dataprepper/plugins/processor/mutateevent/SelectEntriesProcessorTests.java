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
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.opensearch.dataprepper.model.pattern.Pattern;

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
    void invalid_select_when_throws_InvalidPluginConfigurationException() {
        final String selectWhen = UUID.randomUUID().toString();

        when(mockConfig.getSelectWhen()).thenReturn(selectWhen);

        when(expressionEvaluator.isValidExpressionStatement(selectWhen)).thenReturn(false);

        assertThrows(InvalidPluginConfigurationException.class, this::createObjectUnderTest);
    }
    @Test
    public void testSelectEntriesProcessor() {
        when(mockConfig.getIncludeKeys()).thenReturn(List.of("key1", "key2"));
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
    public void testWithKeyDneSelectEntriesProcessor() {
        when(mockConfig.getIncludeKeys()).thenReturn(List.of("key1", "key2"));
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
    public void testSelectEntriesProcessorWithInvalidCondition() {
        final String selectWhen = "/message == \""+UUID.randomUUID().toString()+"\"";
        when(expressionEvaluator.isValidExpressionStatement(selectWhen)).thenReturn(false);
        when(mockConfig.getSelectWhen()).thenReturn(selectWhen);
        assertThrows(InvalidPluginConfigurationException.class, () -> createObjectUnderTest());
        final Record<Event> record = getEvent("thisisamessage");
    }

    @Test
    public void testSelectEntriesProcessorWithCondition() {
        when(mockConfig.getIncludeKeys()).thenReturn(List.of("key1", "key2"));
        final String selectWhen = "/message == \""+UUID.randomUUID().toString()+"\"";
        when(expressionEvaluator.isValidExpressionStatement(selectWhen)).thenReturn(true);
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
    public void testNestedSelectEntriesProcessor() {
        when(mockConfig.getIncludeKeys()).thenReturn(List.of("nested/key1", "nested/nested2/key2"));
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

    @Test
    public void testSelectEntriesProcessorWithIncludeKeysRegex() {
        when(mockConfig.getIncludeKeys()).thenReturn(null);
        when(mockConfig.getIncludeKeysRegex()).thenReturn(List.of(Pattern.compile("include.*"), Pattern.compile("other.*")));
        when(mockConfig.getSelectWhen()).thenReturn(null);
        final String value1 = UUID.randomUUID().toString();
        final String value2 = UUID.randomUUID().toString();
        final SelectEntriesProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        record.getData().put("include-key", value1);
        record.getData().put("include-key2", value2);
        record.getData().put("other-key1", value2);
        record.getData().put("exclude-key", value1);
        record.getData().put("exclude-key2", value2);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));
        assertThat(editedRecords.get(0).getData().containsKey("include-key"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("include-key2"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("other-key1"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("message"), is(false));
        assertThat(editedRecords.get(0).getData().containsKey("exclude-key"), is(false));
        assertThat(editedRecords.get(0).getData().containsKey("exclude-key2"), is(false));

        assertThat(editedRecords.get(0).getData().get("include-key", String.class), equalTo(value1));
        assertThat(editedRecords.get(0).getData().get("include-key2", String.class), equalTo(value2));
    }

    @Test
    public void testSelectEntriesProcessorWithIncludeKeys_and_IncludeKeysRegex() {
        when(mockConfig.getIncludeKeys()).thenReturn(List.of("exclude-key", "message"));
        when(mockConfig.getIncludeKeysRegex()).thenReturn(List.of(Pattern.compile("include.*")));
        when(mockConfig.getSelectWhen()).thenReturn(null);
        final String value1 = UUID.randomUUID().toString();
        final String value2 = UUID.randomUUID().toString();
        final SelectEntriesProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        record.getData().put("include-key", value1);
        record.getData().put("include-key2", value2);
        record.getData().put("exclude-key", value1);
        record.getData().put("exclude-key2", value2);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));
        assertThat(editedRecords.get(0).getData().containsKey("include-key"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("include-key2"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("exclude-key"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("exclude-key2"), is(false));

        assertThat(editedRecords.get(0).getData().get("include-key", String.class), equalTo(value1));
        assertThat(editedRecords.get(0).getData().get("include-key2", String.class), equalTo(value2));
        assertThat(editedRecords.get(0).getData().get("exclude-key", String.class), equalTo(value1));
    }

    @Test
    public void testSelectEntriesProcessorWithIncludeKeysRegexPointerThatDoesNotExist() {
        when(mockConfig.getIncludeKeys()).thenReturn(List.of("message"));
        when(mockConfig.getIncludeKeysRegex()).thenReturn(List.of(Pattern.compile("include.*")));
        when(mockConfig.getSelectWhen()).thenReturn(null);
        when(mockConfig.getIncludeKeysRegexPointer()).thenReturn("/nested");
        final String value1 = UUID.randomUUID().toString();
        final String value2 = UUID.randomUUID().toString();
        final SelectEntriesProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        record.getData().put("include-key", value1);
        record.getData().put("include-key2", value2);
        record.getData().put("exclude-key", value1);
        record.getData().put("exclude-key2", value2);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));
        assertThat(editedRecords.get(0).getData().containsKey("include-key"), is(false));
        assertThat(editedRecords.get(0).getData().containsKey("include-key2"), is(false));
        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("exclude-key"), is(false));
        assertThat(editedRecords.get(0).getData().containsKey("exclude-key2"), is(false));
    }

    @Test
    public void testSelectEntriesProcessorWithIncludeKeysRegexPointerThatDoesExist() {
        when(mockConfig.getIncludeKeys()).thenReturn(Collections.emptyList());
        when(mockConfig.getIncludeKeysRegex()).thenReturn(List.of(Pattern.compile("include.*")));
        when(mockConfig.getSelectWhen()).thenReturn(null);
        when(mockConfig.getIncludeKeysRegexPointer()).thenReturn("/nested");
        final String value1 = UUID.randomUUID().toString();
        final String value2 = UUID.randomUUID().toString();
        final SelectEntriesProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        final Map<String, Object> nestedData = new HashMap<>();
        nestedData.put("include-key", value1);
        nestedData.put("include-key2", value2);
        nestedData.put("exclude-key", value1);
        nestedData.put("exclude-key2", value2);

        record.getData().put("/nested", nestedData);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));
        assertThat(editedRecords.get(0).getData().containsKey("/nested/include-key"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("/nested/include-key2"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("message"), is(false));
        assertThat(editedRecords.get(0).getData().containsKey("/nested/exclude-key"), is(false));
        assertThat(editedRecords.get(0).getData().containsKey("/nested/exclude-key2"), is(false));
    }

    @Test
    public void testSelectEntriesProcessorWithIncludeKeysRegexPointerThatDoesExist_and_include_keys() {
        when(mockConfig.getIncludeKeys()).thenReturn(List.of("message", "/nested/exclude-key"));
        when(mockConfig.getIncludeKeysRegex()).thenReturn(List.of(Pattern.compile("include.*")));
        when(mockConfig.getSelectWhen()).thenReturn(null);
        when(mockConfig.getIncludeKeysRegexPointer()).thenReturn("/nested");
        final String value1 = UUID.randomUUID().toString();
        final String value2 = UUID.randomUUID().toString();
        final SelectEntriesProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        final Map<String, Object> nestedData = new HashMap<>();
        nestedData.put("include-key", value1);
        nestedData.put("include-key2", value2);
        nestedData.put("exclude-key", value1);
        nestedData.put("exclude-key2", value2);

        record.getData().put("/nested", nestedData);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));
        assertThat(editedRecords.get(0).getData().containsKey("/nested/include-key"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("/nested/include-key2"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("/nested/exclude-key"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("exclude-key2"), is(false));
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
