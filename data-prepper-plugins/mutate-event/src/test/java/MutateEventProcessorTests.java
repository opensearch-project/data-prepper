/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.JacksonEvent;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.plugins.processor.mutateevent.MutateEventProcessor;
import com.amazon.dataprepper.plugins.processor.mutateevent.MutateEventProcessorConfig;
import org.junit.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@RunWith(MockitoJUnitRunner.class)
public class MutateEventProcessorTests {
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private MutateEventProcessorConfig mockConfig;

    @InjectMocks
    private MutateEventProcessor processor;

    @Test
    public void testAddMutateEventProcessorTests() {
        when(mockConfig.getRename()).thenReturn(null);
        when(mockConfig.getAdd()).thenReturn(new HashMap<String, Object>() { { put("newMessage", 3); } });
        final Record<Event> record = getMessage("thisisamessage");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
        assertThat(editedRecords.get(0).getData().get("newMessage", Object.class), equalTo(3));
    }

    @Test
    public void testAddNoOverwriteMutateEventProcessorTests() {
        when(mockConfig.getRename()).thenReturn(null);
        when(mockConfig.getAdd()).thenReturn(new HashMap<String, Object>() { { put("newMessage", 3); } });
        when(mockConfig.getOverwriteOnAdd()).thenReturn(false);
        final Record<Event> record = getMessage("thisisamessage");
        record.getData().put("newMessage", "test");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
        assertThat(editedRecords.get(0).getData().get("newMessage", Object.class), equalTo("test"));
    }

    @Test
    public void testRenameMutateEventProcessorTests() {
        when(mockConfig.getRename()).thenReturn(new HashMap<String, String>() { { put("message", "newMessage"); } });
        final Record<Event> record = getMessage("thisisamessage");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("message"), is(false));
        assertThat(editedRecords.get(0).getData().get("newMessage", Object.class), equalTo("thisisamessage"));
    }

    @Test
    public void testRenameNoOverwriteMutateEventProcessorTests() {
        when(mockConfig.getRename()).thenReturn(new HashMap<String, String>() { { put("message", "newMessage"); } });
        when(mockConfig.getOverwriteOnRename()).thenReturn(false);
        final Record<Event> record = getMessage("thisisamessage");
        record.getData().put("newMessage", "test");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("newMessage", Object.class), equalTo("test"));
    }

    @Test
    public void testCopyMutateEventProcessorTests() {
        when(mockConfig.getAdd()).thenReturn(null);
        when(mockConfig.getRename()).thenReturn(null);
        when(mockConfig.getCopy()).thenReturn(new HashMap<String, String>() { { put("message", "newMessage"); } });
        final Record<Event> record = getMessage("thisisamessage");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("newMessage", Object.class), equalTo("thisisamessage"));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo("thisisamessage"));
    }

    @Test
    public void testCopyNoOverwriteMutateEventProcessorTests() {
        when(mockConfig.getAdd()).thenReturn(null);
        when(mockConfig.getRename()).thenReturn(null);
        when(mockConfig.getCopy()).thenReturn(new HashMap<String, String>() { { put("message", "newMessage"); } });
        when(mockConfig.getOverwriteOnCopy()).thenReturn(false);
        final Record<Event> record = getMessage("thisisamessage");
        record.getData().put("newMessage", "test");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("newMessage"), is(true));
        assertThat(editedRecords.get(0).getData().containsKey("message"), is(true));
        assertThat(editedRecords.get(0).getData().get("newMessage", Object.class), equalTo("test"));
        assertThat(editedRecords.get(0).getData().get("message", Object.class), equalTo("thisisamessage"));
    }

    @Test
    public void testDeleteMutateEventProcessorTests() {
        lenient().when(mockConfig.getAdd()).thenReturn(null);
        lenient().when(mockConfig.getRename()).thenReturn(null);
        lenient().when(mockConfig.getCopy()).thenReturn(null);
        when(mockConfig.getDelete()).thenReturn("message");
        final Record<Event> record = getMessage("thisisamessage");
        final List<Record<Event>> editedRecords = (List<Record<Event>>) processor.doExecute(Collections.singletonList(record));

        assertThat(editedRecords.get(0).getData().containsKey("message"), is(false));
    }

    @Test
    public void testIsReadyForShutdownMutateEventProcessorTests() {
        assertThat(processor.isReadyForShutdown(), is(true));
    }

    private Record<Event> getMessage(String message) {
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

    private LinkedHashMap<String, Object> getLinkedHashMap(List<Record<Event>> editedRecords) {
        assertThat(editedRecords.size(), equalTo(1));
        assertThat(editedRecords.get(0), notNullValue());
        LinkedHashMap<String, Object> parsed_message = editedRecords.get(0).getData().get("parsed_message", LinkedHashMap.class);
        assertThat(parsed_message, notNullValue());
        return parsed_message;
    }
}
