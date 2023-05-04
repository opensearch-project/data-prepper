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
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class RenameKeyProcessorTests {
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private RenameKeyProcessorConfig mockConfig;

    @Mock
    private ExpressionEvaluator<Boolean> expressionEvaluator;

    @Test
    public void testSingleOverwriteRenameProcessorTests() {
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("message", "newMessage", true, null)));

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
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("message", "newMessage", false, null)));

        final RenameKeyProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        record.getData().put("newMessage", "test2");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("newMessage", Object.class), equalTo("test2"));
    }

    @Test
    public void testFromKeyDneRenameProcessorTests() {
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("message2", "newMessage", false, null)));

        final RenameKeyProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(false));
        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo("thisisamessage"));
    }

    @Test
    public void testMultiMixedOverwriteRenameProcessorTests() {
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("message", "newMessage", true, null),
                createEntry("message2", "existingMessage", false, null)));

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
        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("message", "newMessage", true, null),
                createEntry("newMessage", "message3", true, null)));

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

        when(mockConfig.getEntries()).thenReturn(createListOfEntries(createEntry("message", "newMessage", false, renameWhen)));

        final RenameKeyProcessor processor = createObjectUnderTest();
        final Record<Event> record = getEvent("thisisamessage");

        when(expressionEvaluator.evaluate(renameWhen, record.getData())).thenReturn(false);

        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(false));
        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo("thisisamessage"));
    }

    private RenameKeyProcessor createObjectUnderTest() {
        return new RenameKeyProcessor(pluginMetrics, mockConfig, expressionEvaluator);
    }

    private RenameKeyProcessorConfig.Entry createEntry(final String fromKey, final String toKey, final boolean overwriteIfToKeyExists, final String renameWhen) {
        return new RenameKeyProcessorConfig.Entry(fromKey, toKey, overwriteIfToKeyExists, renameWhen);
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
