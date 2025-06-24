/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import org.opensearch.dataprepper.event.TestEventKeyFactory;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyFactory;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.common.TransformOption;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;
import org.opensearch.dataprepper.common.TransformOption;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.LinkedList;
import java.util.Arrays;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class RenameKeyProcessorTests {
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private RenameKeyProcessorConfig mockConfig;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    private final EventKeyFactory eventKeyFactory = TestEventKeyFactory.getTestEventFactory();

    @Test
    void invalid_rename_when_throws_InvalidPluginConfigurationException() {
        final String renameWhen = UUID.randomUUID().toString();
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("message", null,"newMessage", true, renameWhen)));


        when(expressionEvaluator.isValidExpressionStatement(renameWhen)).thenReturn(false);

        assertThrows(InvalidPluginConfigurationException.class, this::createObjectUnderTest);
    }

    @Test
    void invalid_config_tests_with_entries_and_transform_options() throws NoSuchFieldException, IllegalAccessException {
        final String renameWhen = UUID.randomUUID().toString();
        List<RenameKeyProcessorConfig.Entry> entries = createListOfEntries(createEntry("message", null,"newMessage", true, renameWhen));

        RenameKeyProcessorConfig renameKeyProcessorConfig = new RenameKeyProcessorConfig();

        ReflectivelySetField.setField(RenameKeyProcessorConfig.class, renameKeyProcessorConfig, "entries", entries);
        ReflectivelySetField.setField(RenameKeyProcessorConfig.class, renameKeyProcessorConfig, "transformOption", TransformOption.LOWERCASE);
        assertFalse(renameKeyProcessorConfig.isValidConfig());
        ReflectivelySetField.setField(RenameKeyProcessorConfig.class, renameKeyProcessorConfig, "transformOption", TransformOption.NONE);
        assertTrue(renameKeyProcessorConfig.isValidConfig());
        ReflectivelySetField.setField(RenameKeyProcessorConfig.class, renameKeyProcessorConfig, "transformOption", TransformOption.LOWERCASE);
        ReflectivelySetField.setField(RenameKeyProcessorConfig.class, renameKeyProcessorConfig, "entries", null);
        assertTrue(renameKeyProcessorConfig.isValidConfig());
        ReflectivelySetField.setField(RenameKeyProcessorConfig.class, renameKeyProcessorConfig, "transformOption", TransformOption.NONE);
        assertFalse(renameKeyProcessorConfig.isValidConfig());
    }

    @Test
    void invalid_config_when_both_from_key_empty_throws_InvalidPluginConfigurationException() {
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry(null,null, "newMessage", true, null)));
        assertThrows(InvalidPluginConfigurationException.class, this::createObjectUnderTest);
    }

    @Test
    void invalid_config_when_both_from_key_set_throws_InvalidPluginConfigurationException() {
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("message","m.*", "newMessage", true, null)));
        assertThrows(InvalidPluginConfigurationException.class, this::createObjectUnderTest);
    }
    @Test
    public void testSingleOverwriteRenameProcessorTests() {
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("message", null,"newMessage", true, null)));

        final RenameKeyProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        record.getData().put("newMessage", "test2");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("message"), is(false));
        assertThat(editedRecords.get(0).getData().get("newMessage", Object.class), equalTo("thisisamessage"));
    }

    @Test
    public void testSingleNoOverwriteRenameProcessorTests() {
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("message", null,"newMessage", false, null)));

        final RenameKeyProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        record.getData().put("newMessage", "test2");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("newMessage", Object.class), equalTo("test2"));
    }

    @Test
    public void testFromKeyPatternNoOverwriteRenameProcessorTests() {
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry(null, "(detailed_timestamp|detail_timestamp).*","detailed_timestamp", false, null)));

        final RenameKeyProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        record.getData().put("detailed_timestamp_1004", "test2");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));
        assertThat(editedRecords.get(0).getData().containsKey("detailed_timestamp"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("detailed_timestamp_1004"), is(false));
        assertThat(editedRecords.get(0).getData().get("detailed_timestamp", Object.class), equalTo("test2"));
    }
    @Test
    public void testFromKeyPatternGroupingPatternRenameProcessorTests() {
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry(null, "(detailed_timestamp|detail_timestamp).*","detailed_timestamp", false, null)));
        final RenameKeyProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        record.getData().put("detailed_timestamp_1004", "test2");
        record.getData().put("test_key","test_value");
        final Record<Event> second_record = getEvent("thisisanewmessage");
        second_record.getData().put("detail_timestamp-123", "test3");
        Collection<Record<Event>> records = new ArrayList<>();
        records.add(record);
        records.add(second_record);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(records);
        assertThat(editedRecords.get(0).getData().containsKey("detailed_timestamp"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("test_key"), is(true));
        assertThat(editedRecords.get(1).getData().containsKey("detailed_timestamp"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("detailed_timestamp_1004"), is(false));
        assertThat(editedRecords.get(1).getData().containsKey("detail_timestamp-123"), is(false));
        assertThat(editedRecords.get(0).getData().get("detailed_timestamp", Object.class), equalTo("test2"));
        assertThat(editedRecords.get(1).getData().get("detailed_timestamp", Object.class), equalTo("test3"));
    }
    @Test
    public void testFromKeyPatternOverwriteRenameProcessorTests() {
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry(null, "me.*","newMessage", true, null)));

        final RenameKeyProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        record.getData().put("newMessage", "test2");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));
        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("message"), is(false));
        assertThat(editedRecords.get(0).getData().get("newMessage", Object.class), equalTo("thisisamessage"));
    }

    @Test
    public void testFromKeyDneRenameProcessorTests() {
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("message2",null, "newMessage", false, null)));

        final RenameKeyProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(false));
        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo("thisisamessage"));
    }

    @Test
    public void testMultiMixedOverwriteRenameProcessorTests() {
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("message",null, "newMessage", true, null),
                createEntry("message2",null, "existingMessage", false, null)));

        final RenameKeyProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        record.getData().put("newMessage", "test2");
        record.getData().put("existingMessage", "test3");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("existingMessage"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("message"), is(false));
        assertThat(editedRecords.get(0).getData().get("newMessage", Object.class), equalTo("thisisamessage"));
        assertThat(editedRecords.get(0).getData().get("existingMessage", Object.class), equalTo("test3"));
    }

    @Test
    public void testChainRenamingRenameProcessorTests() {
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("message",null, "newMessage", true, null),
                createEntry("newMessage", null,"message3", true, null)));

        final RenameKeyProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message3"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(false));
        assertThat(editedRecords.get(0).getData().containsKey("message"), is(false));
        assertThat(editedRecords.get(0).getData().get("message3", Object.class), equalTo("thisisamessage"));
    }

    @Test
    public void testChainRenamingFromKeyPatternRenameProcessorTests() {
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry(null,"me.*", "newMessage", true, null),
                createEntry(null, "new.*","message3", true, null)));

        final RenameKeyProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message3"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(false));
        assertThat(editedRecords.get(0).getData().containsKey("message"), is(false));
        assertThat(editedRecords.get(0).getData().get("message3", Object.class), equalTo("thisisamessage"));
    }

    @Test
    public void testNoRename_when_RenameWhen_returns_false() {
        final String renameWhen = UUID.randomUUID().toString();

        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("message",null, "newMessage", false, renameWhen)));
        when(expressionEvaluator.isValidExpressionStatement(renameWhen)).thenReturn(true);

        final RenameKeyProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");

        when(expressionEvaluator.evaluateConditional(renameWhen, record.getData())).thenReturn(false);

        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(false));
        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo("thisisamessage"));
    }

    @Test
    public void test_transformKey_converting_allkeys_lowercase() {
        final String key1 = "KeY1";
        final String key2 = "kEy2";
        final String key3 = "keY3";
        final String key4 = "key4";
        final String key5 = "KEY5";
        Map<String, Object> data = Map.of(key1, 1, key2, Map.of(key3, Map.of(key4, "value4", key5, 5.555)));
        when(mockConfig.getEntries()).thenReturn(null);
        when(mockConfig.getTransformOption()).thenReturn(TransformOption.LOWERCASE);
        Record<Event> record = buildRecordWithEvent(data);
        final RenameKeyProcessor processor = createObjectUnderTest();
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));
        assertThat(editedRecords.size(), equalTo(1));
        assertThat(editedRecords.get(0).getData().containsKey(key1), is(false));
        assertThat(editedRecords.get(0).getData().containsKey(key2), is(false));
        assertThat(editedRecords.get(0).getData().containsKey(key2+"/"+key3), is(false));
        assertThat(editedRecords.get(0).getData().containsKey(key2+"/"+key3+"/"+key4), is(false));
        assertThat(editedRecords.get(0).getData().containsKey(key2+"/"+key3+"/"+key5), is(false));

        assertThat(editedRecords.get(0).getData().containsKey(key1.toLowerCase()), is(true));
        assertThat(editedRecords.get(0).getData().get(key1.toLowerCase(), Integer.class), equalTo(1));
        assertThat(editedRecords.get(0).getData().containsKey(key2.toLowerCase()), is(true));
        assertThat(editedRecords.get(0).getData().containsKey(key2.toLowerCase()+"/"+key3.toLowerCase()), is(true));
        assertThat(editedRecords.get(0).getData().containsKey(key2.toLowerCase()+"/"+key3.toLowerCase()+"/"+key4.toLowerCase()), is(true));
        assertThat(editedRecords.get(0).getData().get(key2.toLowerCase()+"/"+key3.toLowerCase()+"/"+key4.toLowerCase(), String.class), equalTo("value4"));
        assertThat(editedRecords.get(0).getData().containsKey(key2.toLowerCase()+"/"+key3.toLowerCase()+"/"+key5.toLowerCase()), is(true));
        assertThat(editedRecords.get(0).getData().get(key2.toLowerCase()+"/"+key3.toLowerCase()+"/"+key5.toLowerCase(), String.class), equalTo("5.555"));
    }

    @Test
    public void test_transformKey_converting_allkeys_uppercase() {
        final String key1 = "KeY1";
        final String key2 = "kEy2";
        final String key3 = "keY3";
        final String key4 = "key4";
        final String key5 = "KEY5";
        Map<String, Object> data = Map.of(key1, 1, key2, Map.of(key3, Map.of(key4, "value4", key5, 5.555)));
        when(mockConfig.getEntries()).thenReturn(null);
        when(mockConfig.getTransformOption()).thenReturn(TransformOption.UPPERCASE);
        Record<Event> record = buildRecordWithEvent(data);
        final RenameKeyProcessor processor = createObjectUnderTest();
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));
        assertThat(editedRecords.size(), equalTo(1));
        assertThat(editedRecords.get(0).getData().containsKey(key1), is(false));
        assertThat(editedRecords.get(0).getData().containsKey(key2), is(false));
        assertThat(editedRecords.get(0).getData().containsKey(key2+"/"+key3), is(false));
        assertThat(editedRecords.get(0).getData().containsKey(key2+"/"+key3+"/"+key4), is(false));
        assertThat(editedRecords.get(0).getData().containsKey(key2+"/"+key3+"/"+key5), is(false));

        assertThat(editedRecords.get(0).getData().containsKey(key1.toUpperCase()), is(true));
        assertThat(editedRecords.get(0).getData().get(key1.toUpperCase(), Integer.class), equalTo(1));
        assertThat(editedRecords.get(0).getData().containsKey(key2.toUpperCase()), is(true));
        assertThat(editedRecords.get(0).getData().containsKey(key2.toUpperCase()+"/"+key3.toUpperCase()), is(true));
        assertThat(editedRecords.get(0).getData().containsKey(key2.toUpperCase()+"/"+key3.toUpperCase()+"/"+key4.toUpperCase()), is(true));
        assertThat(editedRecords.get(0).getData().get(key2.toUpperCase()+"/"+key3.toUpperCase()+"/"+key4.toUpperCase(), String.class), equalTo("value4"));
        assertThat(editedRecords.get(0).getData().containsKey(key2.toUpperCase()+"/"+key3.toUpperCase()+"/"+key5.toUpperCase()), is(true));
        assertThat(editedRecords.get(0).getData().get(key2.toUpperCase()+"/"+key3.toUpperCase()+"/"+key5.toUpperCase(), String.class), equalTo("5.555"));
    }

    @Test
    public void test_transformKey_converting_allkeys_capitalize() {
        final String key1 = "key1";
        final String key2 = "key2";
        final String key3 = "key3";
        final String key4 = "key4";
        final String key5 = "key5";
        Map<String, Object> data = Map.of(key1, 1, key2, Map.of(key3, Map.of(key4, "value4", key5, 5.555)));

        when(mockConfig.getEntries()).thenReturn(null);
        when(mockConfig.getTransformOption()).thenReturn(TransformOption.CAPITALIZE);
        Record<Event> record = buildRecordWithEvent(data);
        final RenameKeyProcessor processor = createObjectUnderTest();
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));
        assertThat(editedRecords.size(), equalTo(1));
        assertThat(editedRecords.get(0).getData().containsKey(key1), is(false));
        assertThat(editedRecords.get(0).getData().containsKey(key2), is(false));
        assertThat(editedRecords.get(0).getData().containsKey(key2+"/"+key3), is(false));
        assertThat(editedRecords.get(0).getData().containsKey(key2+"/"+key3+"/"+key4), is(false));
        assertThat(editedRecords.get(0).getData().containsKey(key2+"/"+key3+"/"+key5), is(false));

        assertThat(editedRecords.get(0).getData().containsKey("Key1"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("Key2"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("Key2/Key3"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("Key2/Key3/Key4"), is(true));
        assertThat(editedRecords.get(0).getData().get("Key2/Key3/Key4", String.class), equalTo("value4"));
        assertThat(editedRecords.get(0).getData().containsKey("Key2/Key3/Key5"), is(true));
        assertThat(editedRecords.get(0).getData().get("Key2/Key3/Key5", String.class), equalTo("5.555"));
    }

    private RenameKeyProcessor createObjectUnderTest() {
        return new RenameKeyProcessor(pluginMetrics, mockConfig, expressionEvaluator);
    }

    private RenameKeyProcessorConfig.Entry createEntry(final String fromKey, final String fromKeyPattern, final String toKey, final boolean overwriteIfToKeyExists, final String renameWhen) {
        final EventKey fromEventKey = (fromKey == null) ? null : eventKeyFactory.createEventKey(fromKey);
        final EventKey toEventKey = eventKeyFactory.createEventKey(toKey);
        return new RenameKeyProcessorConfig.Entry(fromEventKey,fromKeyPattern, toEventKey, overwriteIfToKeyExists, renameWhen);
    }

    private List<RenameKeyProcessorConfig.Entry> createListOfEntries(final RenameKeyProcessorConfig.Entry... entries) {
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
