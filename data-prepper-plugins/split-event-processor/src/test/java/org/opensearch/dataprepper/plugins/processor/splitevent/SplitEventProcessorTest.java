/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.splitevent;


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.event.DefaultEventHandle;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;


@ExtendWith(MockitoExtension.class)
public class SplitEventProcessorTest {
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private SplitEventProcessorConfig mockConfig;

    @Mock
    private AcknowledgementSet mockAcknowledgementSet;

    private SplitEventProcessor splitEventProcessor;


   private Record<Event> createTestRecord(final Map<String, Object> data) {

        Event event = JacksonEvent.builder()
               .withData(data)
               .withEventType("event")
               .build();
        
        DefaultEventHandle eventHandle = (DefaultEventHandle) event.getEventHandle();

        eventHandle.addAcknowledgementSet(mockAcknowledgementSet);
        return new Record<>(event);
   }

    @BeforeEach
    void setup() {
         when(mockConfig.getField()).thenReturn("k1");
         when(mockConfig.getDelimiter()).thenReturn(" ");

        splitEventProcessor = new SplitEventProcessor(pluginMetrics, mockConfig);
    }

    private static Stream<Arguments> provideMaps() {
        return Stream.of(
                Arguments.of(
                        Map.of(
                                "k1", "",
                                "k2", "v2"
                        )
                ),
                Arguments.of(
                        Map.of(
                                "k1", "v1",
                                "k2", "v2"
                        )
                ),
                Arguments.of(
                        Map.of("k1", "v1 v2",
                        "k2", "v2"
                        )
                ),
                Arguments.of(
                        Map.of("k1", "v1 v2 v3",
                                "k2", "v2"
                        )
                )
        );
    }

    @Test
    void testHappyPathWithSpaceDelimiter() {
        final Map<String, Object> testData = new HashMap<>();
        testData.put("k1", "v1 v2");
        final Record<Event> record = createTestRecord(testData);
        when(mockConfig.getDelimiter()).thenReturn(" ");
        
        final SplitEventProcessor objectUnderTest = new SplitEventProcessor(pluginMetrics, mockConfig);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) objectUnderTest.doExecute(Collections.singletonList(record));

        for(Record r: editedRecords){
            Event event = (Event) r.getData();
        }

        assertThat(editedRecords.size(), equalTo(2));
        assertThat(editedRecords.get(0).getData().toMap(), equalTo(Map.of("k1","v1")));
        assertThat(editedRecords.get(1).getData().toMap(), equalTo(Map.of("k1","v2")));
    }

        @Test
    void testHappyPathWithSpaceDelimiterForStringWithMultipleSpaces() {
        final Map<String, Object> testData = new HashMap<>();
        testData.put("k1", "v1  v2");
        final Record<Event> record = createTestRecord(testData);
        when(mockConfig.getDelimiter()).thenReturn(" ");
        
        final SplitEventProcessor objectUnderTest = new SplitEventProcessor(pluginMetrics, mockConfig);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) objectUnderTest.doExecute(Collections.singletonList(record));
        for(Record r: editedRecords){
            Event event = (Event) r.getData();
        }

        assertThat(editedRecords.size(), equalTo(3));
        assertThat(editedRecords.get(0).getData().toMap(), equalTo(Map.of("k1","v1")));
        assertThat(editedRecords.get(1).getData().toMap(), equalTo(Map.of("k1","")));
        assertThat(editedRecords.get(2).getData().toMap(), equalTo(Map.of("k1","v2")));
    }

    @Test
    void testHappyPathWithSemiColonDelimiter() {
        final Map<String, Object> testData = new HashMap<>();
        testData.put("k1", "v1;v2");
        final Record<Event> record = createTestRecord(testData);
        when(mockConfig.getDelimiter()).thenReturn(";");

        final SplitEventProcessor objectUnderTest = new SplitEventProcessor(pluginMetrics, mockConfig);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) objectUnderTest.doExecute(Collections.singletonList(record));

        for(Record r: editedRecords){
            Event event = (Event) r.getData();
        }

        assertThat(editedRecords.size(), equalTo(2));
        assertThat(editedRecords.get(0).getData().toMap(), equalTo(Map.of("k1","v1")));
        assertThat(editedRecords.get(1).getData().toMap(), equalTo(Map.of("k1","v2")));
    }

    @Test
    void testHappyPathWithSpaceDelimiterRegex() {
        final Map<String, Object> testData = new HashMap<>();
        testData.put("k1", "v1 v2");
        final Record<Event> record = createTestRecord(testData);
        when(mockConfig.getDelimiter()).thenReturn(null);
        when(mockConfig.getDelimiterRegex()).thenReturn("\\s+");

        final SplitEventProcessor objectUnderTest = new SplitEventProcessor(pluginMetrics, mockConfig);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) objectUnderTest.doExecute(Collections.singletonList(record));

        for(Record r: editedRecords){
            Event event = (Event) r.getData();
        }

        assertThat(editedRecords.size(), equalTo(2));
        assertThat(editedRecords.get(0).getData().toMap(), equalTo(Map.of("k1","v1")));
        assertThat(editedRecords.get(1).getData().toMap(), equalTo(Map.of("k1","v2")));
    }

    @Test
    void testHappyPathWithColonDelimiterRegex() {
        final Map<String, Object> testData = new HashMap<>();
        testData.put("k1", "v1:v2");
        final Record<Event> record = createTestRecord(testData);
        when(mockConfig.getDelimiter()).thenReturn(null);
        when(mockConfig.getDelimiterRegex()).thenReturn(":");

        final SplitEventProcessor objectUnderTest = new SplitEventProcessor(pluginMetrics, mockConfig);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) objectUnderTest.doExecute(Collections.singletonList(record));

        for(Record r: editedRecords){
            Event event = (Event) r.getData();
        }

        assertThat(editedRecords.size(), equalTo(2));
        assertThat(editedRecords.get(0).getData().toMap(), equalTo(Map.of("k1","v1")));
        assertThat(editedRecords.get(1).getData().toMap(), equalTo(Map.of("k1","v2")));
    }

    @Test
    void testFailureWithBothDelimiterRegexAndDelimiterDefined() {
        when(mockConfig.getDelimiter()).thenReturn(" ");
        when(mockConfig.getDelimiterRegex()).thenReturn("\\s+");
        assertThrows(IllegalArgumentException.class, () -> new SplitEventProcessor(pluginMetrics, mockConfig));
    }

    @Test
    void testFailureWithDelimiterRegexAndDelimiterDefinedMissing() {
        when(mockConfig.getDelimiter()).thenReturn(null);
        when(mockConfig.getDelimiterRegex()).thenReturn(null);
        assertThrows(IllegalArgumentException.class, () -> new SplitEventProcessor(pluginMetrics, mockConfig));
    }

    @ParameterizedTest
    @MethodSource("provideMaps")
    void testSplitEventsBelongToTheSameAcknowledgementSet(Map<String, Object> inputMap1) {
        final Record<Event> testRecord = createTestRecord(inputMap1);
        Event originalEvent = testRecord.getData();
        DefaultEventHandle originalEventHandle = (DefaultEventHandle) originalEvent.getEventHandle();
        AcknowledgementSet originalAcknowledgementSet= originalEventHandle.getAcknowledgementSet();

        final SplitEventProcessor objectUnderTest = new SplitEventProcessor(pluginMetrics, mockConfig);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) objectUnderTest.doExecute(Collections.singletonList(testRecord));

        DefaultEventHandle eventHandle = (DefaultEventHandle) originalEvent.getEventHandle();
        AcknowledgementSet acknowledgementSet;
        for(Record record: editedRecords) {
            Event event = testRecord.getData();
            eventHandle = (DefaultEventHandle) event.getEventHandle();
            acknowledgementSet = eventHandle.getAcknowledgementSet();
            assertEquals(originalAcknowledgementSet, acknowledgementSet);
        }
    }

    @Test
    void testSplitEventsWhenNoSplits() {
        final Map<String, Object> testData = new HashMap<>();
        List<Record<Event>> records = new ArrayList<>();

        testData.put("k1", "v1");
        testData.put("k2", "v2");
        final Record<Event> record1 = createTestRecord(testData);

        final Map<String, Object> testData2 = new HashMap<>();
        testData2.put("k1", "");
        testData2.put("k3", "v3");
        final Record<Event> record2 = createTestRecord(testData2);

        records.add(record1);
        records.add(record2);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) splitEventProcessor.doExecute(records);

        assertThat(editedRecords.size(), equalTo(2));
        assertThat(editedRecords.get(0).getData().toMap(), equalTo(Map.of("k1","v1", "k2", "v2")));
        assertThat(editedRecords.get(1).getData().toMap(), equalTo(Map.of("k1","", "k3", "v3")));
    }


    @Test
    void testSplitEventsWhenOneSplit() {
        List<Record<Event>> records = new ArrayList<>();
        final Map<String, Object> testData = new HashMap<>();
        testData.put("k1", "v1 v2");
        testData.put("k2", "v3");
        final Record<Event> record = createTestRecord(testData);
        records.add(record);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) splitEventProcessor.doExecute(records);

        assertThat(editedRecords.size(), equalTo(2));
        assertThat(editedRecords.get(0).getData().toMap(), equalTo(Map.of("k1","v1", "k2", "v3")));
        assertThat(editedRecords.get(1).getData().toMap(), equalTo(Map.of("k1","v2", "k2", "v3")));
    }


    @Test
    void testSplitEventsWhenMultipleSplits() {
        List<Record<Event>> records = new ArrayList<>();
        final Map<String, Object> testData = new HashMap<>();
        testData.put("k1", "v1 v2 v3 v4");
        testData.put("k2", "v5");
        final Record<Event> record = createTestRecord(testData);
        records.add(record);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) splitEventProcessor.doExecute(records);

        assertThat(editedRecords.size(), equalTo(4));
        assertThat(editedRecords.get(0).getData().toMap(), equalTo(Map.of("k1","v1", "k2", "v5")));
        assertThat(editedRecords.get(1).getData().toMap(), equalTo(Map.of("k1","v2", "k2", "v5")));
        assertThat(editedRecords.get(2).getData().toMap(), equalTo(Map.of("k1","v3", "k2", "v5")));
        assertThat(editedRecords.get(3).getData().toMap(), equalTo(Map.of("k1","v4", "k2", "v5")));
    }

    @Test
    void testSplitEventsWhenMultipleSplitsMultipleRecords() {
        List<Record<Event>> records = new ArrayList<>();
        final Map<String, Object> testData1 = new HashMap<>();
        testData1.put("k1", "v1 v2");
        testData1.put("k2", "v5");
        final Record<Event> record = createTestRecord(testData1);
        records.add(record);

        final Map<String, Object> testData2 = new HashMap<>();
        testData2.put("k1", "v1");
        testData2.put("k2", "v3");
        final Record<Event> record2 = createTestRecord(testData2);
        records.add(record2);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) splitEventProcessor.doExecute(records);

        assertThat(editedRecords.size(), equalTo(3));
        assertThat(editedRecords.get(0).getData().toMap(), equalTo(Map.of("k1","v1", "k2", "v5")));
        assertThat(editedRecords.get(1).getData().toMap(), equalTo(Map.of("k1","v2", "k2", "v5")));
        assertThat(editedRecords.get(2).getData().toMap(), equalTo(Map.of("k1","v1", "k2", "v3")));
    }

    @Test
    void testSplitEventsWhenNoKeyPresentInEvent() {
        List<Record<Event>> records = new ArrayList<>();
        final Map<String, Object> testData1 = new HashMap<>();
        testData1.put("k2", "v5 v6");
        final Record<Event> record = createTestRecord(testData1);
        records.add(record);

        final Map<String, Object> testData2 = new HashMap<>();
        testData2.put("k3", "v3 v5");
        final Record<Event> record2 = createTestRecord(testData2);
        records.add(record2);
        final List<Record<Event>> editedRecords = (List<Record<Event>>) splitEventProcessor.doExecute(records);

        assertThat(editedRecords.size(), equalTo(2));
        assertThat(editedRecords.get(0).getData().toMap(), equalTo(Map.of("k2", "v5 v6")));
        assertThat(editedRecords.get(1).getData().toMap(), equalTo(Map.of("k3", "v3 v5")));
    }

    @Test
    public void testCreateNewRecordFromEvent() {
        Event recordEvent = mock(Event.class);

        Map<String, Object> eventData = new HashMap<>();
        eventData.put("someField", "someValue");
        when(recordEvent.toMap()).thenReturn(eventData);
        when(recordEvent.getMetadata()).thenReturn(mock(EventMetadata.class));
        String splitValue = "splitValue";

        Record resultRecord = splitEventProcessor.createNewRecordFromEvent(recordEvent, splitValue);
        Event editedEvent = (Event) resultRecord.getData();
        // Assertions
        assertEquals(editedEvent.getMetadata(),recordEvent.getMetadata());
    }

    @Test
    public void testAddToAcknowledgementSetFromOriginEvent() {
        Map<String, Object> data = Map.of("k1","v1");
        EventMetadata eventMetadata = mock(EventMetadata.class);
        Event originRecordEvent = JacksonEvent.builder()
                .withEventMetadata(eventMetadata)
                .withEventType("event")
                .withData(data)
                .build();
        Event spyEvent = spy(originRecordEvent);

        DefaultEventHandle mockEventHandle = mock(DefaultEventHandle.class);
        when(spyEvent.getEventHandle()).thenReturn(mockEventHandle);

        Record record = splitEventProcessor
                .createNewRecordFromEvent(spyEvent, "v1");

        Event recordEvent = (Event) record.getData();
        splitEventProcessor.addToAcknowledgementSetFromOriginEvent(recordEvent, spyEvent);

        DefaultEventHandle spyEventHandle = (DefaultEventHandle) spyEvent.getEventHandle();
        // Verify that the add method is called on the acknowledgement set
        verify(spyEventHandle).getAcknowledgementSet();

        AcknowledgementSet spyAckSet = spyEventHandle.getAcknowledgementSet();
        DefaultEventHandle eventHandle = (DefaultEventHandle) recordEvent.getEventHandle();
        AcknowledgementSet ackSet1 = eventHandle.getAcknowledgementSet();

        assertEquals(spyAckSet, ackSet1);
    }

    @Test
    void testIsReadyForShutdown() {
        assertTrue(splitEventProcessor.isReadyForShutdown());
    }
    
}
