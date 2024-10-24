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
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.record.Record;
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
