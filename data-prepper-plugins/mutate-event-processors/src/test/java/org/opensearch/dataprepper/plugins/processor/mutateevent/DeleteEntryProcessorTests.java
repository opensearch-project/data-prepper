/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.event.TestEventKeyFactory;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventKeyFactory;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.record.Record;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DeleteEntryProcessorTests {
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private DeleteEntryProcessorConfig mockConfig;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    private final EventKeyFactory eventKeyFactory = TestEventKeyFactory.getTestEventFactory();

    @Test
    void invalid_delete_when_throws_InvalidPluginConfigurationException() {
        final String deleteWhen = UUID.randomUUID().toString();

        when(mockConfig.getDeleteWhen()).thenReturn(deleteWhen);

        when(expressionEvaluator.isValidExpressionStatement(deleteWhen)).thenReturn(false);

        assertThrows(InvalidPluginConfigurationException.class, this::createObjectUnderTest);
    }

    @Test
    public void testSingleDeleteProcessorTest() {
        when(mockConfig.getWithKeys()).thenReturn(List.of(eventKeyFactory.createEventKey("message", EventKeyFactory.EventAction.DELETE)));
        when(mockConfig.getDeleteWhen()).thenReturn(null);

        final DeleteEntryProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        record.getData().put("newMessage", "test");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(false));
        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
    }

    @Test
    public void testSingleEntryIterativeDeleteKey() {
        final String testKey = "testKey";
        when(mockConfig.getWithKeys()).thenReturn(List.of(eventKeyFactory.createEventKey(testKey, EventKeyFactory.EventAction.DELETE)));
        when(mockConfig.getIterateOn()).thenReturn("message");
        when(mockConfig.getDeleteWhen()).thenReturn(null);

        final DeleteEntryProcessor processor = createObjectUnderTest();
        final List<Map<String, Object>> mapList = List.of(Map.of(testKey, UUID.randomUUID().toString()));
        final Map<String, Object> data = Map.of("message", mapList);
        final Record<Event> record = buildRecordWithEvent(data);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", List.class), equalTo(
                List.of(Collections.emptyMap())));
    }

    @Test
    void invalid_delete_from_element_when_throws_InvalidPluginConfigurationException() {
        final String testKey = UUID.randomUUID().toString();
        final String deleteWhen = UUID.randomUUID().toString();
        final String deleteFromElementWhen = UUID.randomUUID().toString();

        when(mockConfig.getWithKeys()).thenReturn(List.of(eventKeyFactory.createEventKey(testKey, EventKeyFactory.EventAction.DELETE)));
        when(mockConfig.getIterateOn()).thenReturn("message");
        when(mockConfig.getDeleteWhen()).thenReturn(deleteWhen);
        when(mockConfig.getDeleteFromElementWhen()).thenReturn(deleteFromElementWhen);

        when(expressionEvaluator.isValidExpressionStatement(deleteWhen)).thenReturn(true);
        when(expressionEvaluator.isValidExpressionStatement(deleteFromElementWhen)).thenReturn(false);

        assertThrows(InvalidPluginConfigurationException.class, this::createObjectUnderTest);
    }

    @Test
    void using_delete_from_element_when_without_iterate_on_throws_InvalidPluginConfigurationException() {
        final String testKey = UUID.randomUUID().toString();
        final String deleteWhen = UUID.randomUUID().toString();
        final String deleteFromElementWhen = UUID.randomUUID().toString();

        when(mockConfig.getWithKeys()).thenReturn(List.of(eventKeyFactory.createEventKey(testKey, EventKeyFactory.EventAction.DELETE)));
        when(mockConfig.getIterateOn()).thenReturn(null);
        when(mockConfig.getDeleteWhen()).thenReturn(deleteWhen);
        when(mockConfig.getDeleteFromElementWhen()).thenReturn(deleteFromElementWhen);

        when(expressionEvaluator.isValidExpressionStatement(deleteWhen)).thenReturn(true);

        assertThrows(InvalidPluginConfigurationException.class, this::createObjectUnderTest);
    }

    @Test
    public void testSingleEntryIterativeDeleteKey_applyEventLevelDeleteWhen_when_deleteWhen_returns_true() {
        final String testKey = "testKey";
        final String deleteWhen = "/condition == true";
        when(expressionEvaluator.isValidExpressionStatement(deleteWhen)).thenReturn(true);
        when(expressionEvaluator.evaluateConditional(eq(deleteWhen), any(Event.class))).thenAnswer(invocation -> {
            Event eventArg = invocation.getArgument(1);
            return eventArg.get("condition", Boolean.class);
        });
        when(mockConfig.getWithKeys()).thenReturn(List.of(eventKeyFactory.createEventKey(testKey, EventKeyFactory.EventAction.DELETE)));
        when(mockConfig.getIterateOn()).thenReturn("message");
        when(mockConfig.getDeleteWhen()).thenReturn(deleteWhen);

        final DeleteEntryProcessor processor = createObjectUnderTest();
        final List<Map<String, Object>> mapList = List.of(Map.of(testKey, UUID.randomUUID().toString()));
        final Map<String, Object> data = Map.of(
                "condition", true,
                "message", mapList
        );
        final Record<Event> record = buildRecordWithEvent(data);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("condition"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", List.class), equalTo(
                List.of(Collections.emptyMap())));
    }

    @Test
    public void iterate_on_with_delete_from_element_when_condition_true_deletes_key() {
        final String testKey = "testKey";
        final String deleteWhen = "/condition == true";
        final String deleteFromElementWhen = UUID.randomUUID().toString();

        when(mockConfig.getWithKeys()).thenReturn(List.of(eventKeyFactory.createEventKey(testKey, EventKeyFactory.EventAction.DELETE)));
        when(mockConfig.getIterateOn()).thenReturn("message");
        when(mockConfig.getDeleteWhen()).thenReturn(deleteWhen);
        when(mockConfig.getDeleteFromElementWhen()).thenReturn(deleteFromElementWhen);

        when(expressionEvaluator.isValidExpressionStatement(deleteWhen)).thenReturn(true);
        when(expressionEvaluator.isValidExpressionStatement(deleteFromElementWhen)).thenReturn(true);

        when(expressionEvaluator.evaluateConditional(eq(deleteWhen), any(Event.class))).thenReturn(true);
        when(expressionEvaluator.evaluateConditional(eq(deleteFromElementWhen), any(Event.class))).thenReturn(true);

        final DeleteEntryProcessor processor = createObjectUnderTest();
        final List<Map<String, Object>> mapList = List.of(Map.of(
                "condition", true,
                testKey, UUID.randomUUID().toString()));
        final Map<String, Object> data = Map.of("message", mapList);
        final Record<Event> record = buildRecordWithEvent(data);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", List.class), equalTo(
                List.of(Map.of("condition", true))));
    }

    @Test
    public void iterate_on_with_delete_from_element_when_condition_false_does_not_delete() {
        final String testKey = "testKey";
        final String deleteWhen = "/condition == true";
        final String deleteFromElementWhen = UUID.randomUUID().toString();

        when(mockConfig.getWithKeys()).thenReturn(List.of(eventKeyFactory.createEventKey(testKey, EventKeyFactory.EventAction.DELETE)));
        when(mockConfig.getIterateOn()).thenReturn("message");
        when(mockConfig.getDeleteWhen()).thenReturn(deleteWhen);
        when(mockConfig.getDeleteFromElementWhen()).thenReturn(deleteFromElementWhen);

        when(expressionEvaluator.isValidExpressionStatement(deleteWhen)).thenReturn(true);
        when(expressionEvaluator.isValidExpressionStatement(deleteFromElementWhen)).thenReturn(true);

        when(expressionEvaluator.evaluateConditional(eq(deleteWhen), any(Event.class))).thenReturn(true);
        when(expressionEvaluator.evaluateConditional(eq(deleteFromElementWhen), any(Event.class))).thenReturn(false);

        final DeleteEntryProcessor processor = createObjectUnderTest();
        final List<Map<String, Object>> mapList = List.of(Map.of(
                "condition", true,
                testKey, UUID.randomUUID().toString()));
        final Map<String, Object> data = Map.of("message", mapList);
        final Record<Event> record = buildRecordWithEvent(data);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", List.class), equalTo(mapList));
    }

    @Test
    public void iterate_on_with_delete_when_condition_false_does_not_delete() {
        final String testKey = "testKey";
        final String deleteWhen = "/condition == true";
        final String deleteFromElementWhen = UUID.randomUUID().toString();
        when(expressionEvaluator.isValidExpressionStatement(deleteWhen)).thenReturn(true);
        when(expressionEvaluator.isValidExpressionStatement(deleteFromElementWhen)).thenReturn(true);

        when(mockConfig.getWithKeys()).thenReturn(List.of(eventKeyFactory.createEventKey(testKey, EventKeyFactory.EventAction.DELETE)));
        when(mockConfig.getIterateOn()).thenReturn("message");
        when(mockConfig.getDeleteWhen()).thenReturn(deleteWhen);
        when(mockConfig.getDeleteFromElementWhen()).thenReturn(deleteFromElementWhen);

        when(expressionEvaluator.evaluateConditional(eq(deleteWhen), any(Event.class))).thenReturn(false);


        final DeleteEntryProcessor processor = createObjectUnderTest();
        final List<Map<String, Object>> mapList = List.of(Map.of(
                "condition", false,
                testKey, UUID.randomUUID().toString()));
        final Map<String, Object> data = Map.of("message", mapList);
        final Record<Event> record = buildRecordWithEvent(data);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", List.class), equalTo(mapList));

        verifyNoMoreInteractions(expressionEvaluator);
    }

    @Test
    public void testWithKeyOneDeleteProcessorTest() {
        when(mockConfig.getWithKeys()).thenReturn(List.of(eventKeyFactory.createEventKey("message2", EventKeyFactory.EventAction.DELETE)));
        when(mockConfig.getDeleteWhen()).thenReturn(null);

        final DeleteEntryProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        record.getData().put("newMessage", "test");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("message2"), is(false));
    }

    @Test
    public void testMultiDeleteProcessorTest() {
        when(mockConfig.getWithKeys()).thenReturn(List.of(
                eventKeyFactory.createEventKey("message", EventKeyFactory.EventAction.DELETE),
                eventKeyFactory.createEventKey("message2", EventKeyFactory.EventAction.DELETE)));
        when(mockConfig.getDeleteWhen()).thenReturn(null);

        final DeleteEntryProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        record.getData().put("message2", "test");
        record.getData().put("newMessage", "test");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(false));
        assertThat(editedRecords.get(0).getData().containsKey("message2"), is(false));
        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
    }

    @Test
    public void testKeyIsNotDeleted_when_deleteWhen_returns_false() {
        when(mockConfig.getWithKeys()).thenReturn(List.of(eventKeyFactory.createEventKey("message", EventKeyFactory.EventAction.DELETE)));
        final String deleteWhen = UUID.randomUUID().toString();
        when(mockConfig.getDeleteWhen()).thenReturn(deleteWhen);
        when(expressionEvaluator.isValidExpressionStatement(deleteWhen)).thenReturn(true);

        final DeleteEntryProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        record.getData().put("newMessage", "test");

        when(expressionEvaluator.evaluateConditional(deleteWhen, record.getData())).thenReturn(false);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
    }

    @Test
    public void testNestedDeleteProcessorTest() {
        when(mockConfig.getWithKeys()).thenReturn(List.of(eventKeyFactory.createEventKey("nested/foo", EventKeyFactory.EventAction.DELETE)));

        Map<String, Object> nested = Map.of("foo", "bar", "fizz", 42);

        final DeleteEntryProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        record.getData().put("nested", nested);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("nested/foo"), is(false));
        assertThat(editedRecords.get(0).getData().containsKey("nested/fizz"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
    }

    @Test
    public void test_multiple_entries_with_different_delete_when_conditions() {
        final DeleteEntryProcessorConfig.Entry entry1 = new DeleteEntryProcessorConfig.Entry(
                List.of(eventKeyFactory.createEventKey("key1", EventKeyFactory.EventAction.DELETE)),
                null, null, "condition1", null, null);
        final DeleteEntryProcessorConfig.Entry entry2 = new DeleteEntryProcessorConfig.Entry(
                List.of(eventKeyFactory.createEventKey("key2", EventKeyFactory.EventAction.DELETE)),
                null, null, "condition2", null, null);

        when(mockConfig.getEntries()).thenReturn(List.of(entry1, entry2));
        when(expressionEvaluator.isValidExpressionStatement("condition1")).thenReturn(true);
        when(expressionEvaluator.isValidExpressionStatement("condition2")).thenReturn(true);

        final DeleteEntryProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("test");
        record.getData().put("key1", "value1");
        record.getData().put("key2", "value2");

        when(expressionEvaluator.evaluateConditional("condition1", record.getData())).thenReturn(true);
        when(expressionEvaluator.evaluateConditional("condition2", record.getData())).thenReturn(false);

        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("key1"), is(false));
        assertThat(editedRecords.get(0).getData().containsKey("key2"), is(true));
    }

    @Test
    public void test_legacy_format_conversion_to_entries_format() {
        when(mockConfig.getWithKeys()).thenReturn(List.of(eventKeyFactory.createEventKey("message", EventKeyFactory.EventAction.DELETE)));
        when(mockConfig.getDeleteWhen()).thenReturn("condition");
        when(expressionEvaluator.isValidExpressionStatement("condition")).thenReturn(true);

        final DeleteEntryProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("test");

        when(expressionEvaluator.evaluateConditional("condition", record.getData())).thenReturn(true);

        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(false));
    }

    @Test
    public void invalid_delete_when_with_entries_format_throws_InvalidPluginConfigurationException() {
        DeleteEntryProcessorConfig.Entry entry = new DeleteEntryProcessorConfig.Entry(
                List.of(eventKeyFactory.createEventKey("key1", EventKeyFactory.EventAction.DELETE)),
                null, null, "invalid_condition", null, null);

        when(mockConfig.getEntries()).thenReturn(List.of(entry));
        when(expressionEvaluator.isValidExpressionStatement("invalid_condition")).thenReturn(false);

        assertThrows(InvalidPluginConfigurationException.class, this::createObjectUnderTest);
    }

    @Test
    public void test_both_configurations_used_together() {
        final DeleteEntryProcessorConfig configObjectUnderTest = new DeleteEntryProcessorConfig();
        final DeleteEntryProcessorConfig.Entry entry = new DeleteEntryProcessorConfig.Entry(
                List.of(eventKeyFactory.createEventKey("key1", EventKeyFactory.EventAction.DELETE)),
                null, null, "condition", null, null);

        ReflectionTestUtils.setField(configObjectUnderTest, "withKeys", List.of(eventKeyFactory.createEventKey("message",
                EventKeyFactory.EventAction.DELETE)));
        ReflectionTestUtils.setField(configObjectUnderTest, "entries", List.of(entry));

        assertThat(configObjectUnderTest.hasOnlyOneConfiguration(), is(false));
    }

    @Test
    public void test_no_configuration_used() {
        final DeleteEntryProcessorConfig configObjectUnderTest = new DeleteEntryProcessorConfig();

        ReflectionTestUtils.setField(configObjectUnderTest, "withKeys", null);
        ReflectionTestUtils.setField(configObjectUnderTest, "withKeysRegex", null);
        ReflectionTestUtils.setField(configObjectUnderTest, "entries", null);

        assertThat(configObjectUnderTest.isConfigurationPresent(), is(false));
    }


    @Test
    public void test_has_only_one_config_returns_false_when_entries_and_with_keys_regex_used_together() {
        final DeleteEntryProcessorConfig configObjectUnderTest = new DeleteEntryProcessorConfig();
        final DeleteEntryProcessorConfig.Entry entry = new DeleteEntryProcessorConfig.Entry(
                null, List.of("^ran.*"), null, "condition", null, null);

        ReflectionTestUtils.setField(configObjectUnderTest, "withKeysRegex", List.of("^test.*"));
        ReflectionTestUtils.setField(configObjectUnderTest, "entries", List.of(entry));

        assertThat(configObjectUnderTest.hasOnlyOneConfiguration(), is(false));
    }

    @Test
    public void test_has_only_one_config_returns_false_when_with_keys_and_with_keys_regex_used_together() {
        final DeleteEntryProcessorConfig configObjectUnderTest = new DeleteEntryProcessorConfig();

        ReflectionTestUtils.setField(configObjectUnderTest, "withKeys", List.of(
                eventKeyFactory.createEventKey("key1",EventKeyFactory.EventAction.DELETE)));
        ReflectionTestUtils.setField(configObjectUnderTest, "withKeysRegex", List.of("^test.*"));

        assertThat(configObjectUnderTest.hasOnlyOneConfiguration(), is(false));
    }

    @Test
    public void test_exclude_from_delete_without_with_key_regex() {
        final DeleteEntryProcessorConfig configObjectUnderTest = new DeleteEntryProcessorConfig();

        ReflectionTestUtils.setField(configObjectUnderTest, "withKeys", List.of(
                eventKeyFactory.createEventKey("key1", EventKeyFactory.EventAction.DELETE)));
        ReflectionTestUtils.setField(configObjectUnderTest, "excludeFromDelete", Set.of(
                eventKeyFactory.createEventKey("excludeKey")));

        assertThat(configObjectUnderTest.isExcludeFromDeleteValid(), is(false));
    }

    @Test
    public void test_one_event_deletion_using_with_keys_regex() {
        when(mockConfig.getWithKeysRegex()).thenReturn(List.of("^ran.*"));

        final DeleteEntryProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("message");
        record.getData().put("random_key", "test");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("random_key"), is(false));
    }

    @Test
    public void test_multiple_events_deletion_using_with_keys_regex() {
        when(mockConfig.getWithKeysRegex()).thenReturn(List.of("^ran.*"));

        final DeleteEntryProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("message");
        record.getData().put("random", "test");
        record.getData().put("arrange", "test");
        record.getData().put("range", "test");
        record.getData().put("ran", "test");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("random"), is(false));
        assertThat(editedRecords.get(0).getData().containsKey("arrange"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("range"), is(false));
        assertThat(editedRecords.get(0).getData().containsKey("ran"), is(false));
    }

    @Test
    public void test_with_keys_regex_and_exclude_from_delete_used_together() {
        when(mockConfig.getWithKeysRegex()).thenReturn(List.of("^ran.*"));
        when(mockConfig.getExcludeFromDelete()).thenReturn(Set.of(eventKeyFactory.createEventKey("random_key")));

        final DeleteEntryProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("message");
        record.getData().put("random_key", "test");
        record.getData().put("randomKey", "test2");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("random_key"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("randomKey"), is(false));
    }

    @Test
    public void test_with_keys_regex_and_iterative_key_used_together() {
        final String testKey = "testKey";
        when(mockConfig.getWithKeysRegex()).thenReturn(List.of("^test.*"));
        when(mockConfig.getIterateOn()).thenReturn("message");
        when(mockConfig.getDeleteWhen()).thenReturn(null);

        final DeleteEntryProcessor processor = createObjectUnderTest();
        final List<Map<String, Object>> mapList = List.of(Map.of(testKey, UUID.randomUUID().toString()));
        final Map<String, Object> data = Map.of("message", mapList);
        final Record<Event> record = buildRecordWithEvent(data);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", List.class), equalTo(
                List.of(Collections.emptyMap())));
    }

    @Test
    public void test_with_keys_regex_and_iterate_on_with_delete_from_element_when_condition_true_deletes_key() {
        final String testKey = "testKey";
        final String deleteWhen = "/condition == true";
        final String deleteFromElementWhen = UUID.randomUUID().toString();

        when(mockConfig.getWithKeysRegex()).thenReturn(List.of("^test.*"));
        when(mockConfig.getIterateOn()).thenReturn("message");
        when(mockConfig.getDeleteWhen()).thenReturn(deleteWhen);
        when(mockConfig.getDeleteFromElementWhen()).thenReturn(deleteFromElementWhen);

        when(expressionEvaluator.isValidExpressionStatement(deleteWhen)).thenReturn(true);
        when(expressionEvaluator.isValidExpressionStatement(deleteFromElementWhen)).thenReturn(true);

        when(expressionEvaluator.evaluateConditional(eq(deleteWhen), any(Event.class))).thenReturn(true);
        when(expressionEvaluator.evaluateConditional(eq(deleteFromElementWhen), any(Event.class))).thenReturn(true);

        final DeleteEntryProcessor processor = createObjectUnderTest();
        final List<Map<String, Object>> mapList = List.of(Map.of(
                "condition", true,
                testKey, UUID.randomUUID().toString()));
        final Map<String, Object> data = Map.of("message", mapList);
        final Record<Event> record = buildRecordWithEvent(data);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", List.class), equalTo(
                List.of(Map.of("condition", true))));
    }

    @Test
    public void testSingleEntryIterativeDeleteKeyRegex_applyEventLevelDeleteWhen_when_deleteWhen_returns_true() {
        final String testKey = "testKey";
        final String deleteWhen = "/condition == true";
        when(expressionEvaluator.isValidExpressionStatement(deleteWhen)).thenReturn(true);
        when(expressionEvaluator.evaluateConditional(eq(deleteWhen), any(Event.class))).thenAnswer(invocation -> {
            Event eventArg = invocation.getArgument(1);
            return eventArg.get("condition", Boolean.class);
        });
        when(mockConfig.getWithKeysRegex()).thenReturn(List.of("^test.*"));
        when(mockConfig.getIterateOn()).thenReturn("message");
        when(mockConfig.getDeleteWhen()).thenReturn(deleteWhen);

        final DeleteEntryProcessor processor = createObjectUnderTest();
        final List<Map<String, Object>> mapList = List.of(Map.of(testKey, UUID.randomUUID().toString()));
        final Map<String, Object> data = Map.of(
                "condition", true,
                "message", mapList
        );
        final Record<Event> record = buildRecordWithEvent(data);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("condition"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", List.class), equalTo(
                List.of(Collections.emptyMap())));
    }

    @Test
    public void test_multiple_entries_with_withKey_and_withKeyRegex_configs() {
        final DeleteEntryProcessorConfig.Entry entry1 = new DeleteEntryProcessorConfig.Entry(List.of(
                eventKeyFactory.createEventKey("key1", EventKeyFactory.EventAction.DELETE),
                eventKeyFactory.createEventKey("key2", EventKeyFactory.EventAction.DELETE)),
                null, null, null, null, null);
        final DeleteEntryProcessorConfig.Entry entry2 = new DeleteEntryProcessorConfig.Entry(
                null, List.of("ran.*"), Set.of(eventKeyFactory.createEventKey("random")), null, null, null);
        final DeleteEntryProcessorConfig.Entry entry3 = new DeleteEntryProcessorConfig.Entry(
                null, List.of("test.*"), null, null, null, null);

        when(mockConfig.getEntries()).thenReturn(List.of(entry1, entry2, entry3));

        final DeleteEntryProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("test");
        record.getData().put("key1", "value1");
        record.getData().put("key2", "value2");
        record.getData().put("randomKey", "value3");
        record.getData().put("random", "value4");
        record.getData().put("testKey", "value5");

        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("key1"), is(false));
        assertThat(editedRecords.get(0).getData().containsKey("key2"), is(false));
        assertThat(editedRecords.get(0).getData().containsKey("randomKey"), is(false));
        assertThat(editedRecords.get(0).getData().containsKey("random"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("testKey"), is(false));
    }

    @Test
    public void test_multi_delete_processor_using_with_keys_regex() {
        when(mockConfig.getWithKeysRegex()).thenReturn(List.of("^ran.*", "^test.*"));

        final DeleteEntryProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("message");
        record.getData().put("testKey", "test");
        record.getData().put("randomKey", "test");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("testKey"), is(false));
        assertThat(editedRecords.get(0).getData().containsKey("randomKey"), is(false));
        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
    }

    @Test
    public void testKeyIsNotDeleted_when_deleteWhen_returns_false_using_with_keys_regex() {
        when(mockConfig.getWithKeysRegex()).thenReturn(List.of("^test.*"));
        final String deleteWhen = UUID.randomUUID().toString();
        when(mockConfig.getDeleteWhen()).thenReturn(deleteWhen);
        when(expressionEvaluator.isValidExpressionStatement(deleteWhen)).thenReturn(true);

        final DeleteEntryProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("message");
        record.getData().put("testKey", "test");

        when(expressionEvaluator.evaluateConditional(deleteWhen, record.getData())).thenReturn(false);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("testKey"), is(true));
    }

    private DeleteEntryProcessor createObjectUnderTest() {
        return new DeleteEntryProcessor(pluginMetrics, mockConfig, expressionEvaluator);
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
