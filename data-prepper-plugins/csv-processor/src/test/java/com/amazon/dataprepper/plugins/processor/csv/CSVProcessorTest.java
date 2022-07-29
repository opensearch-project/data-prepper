/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.csv;

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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CSVProcessorTest {
    @Mock
    private CSVProcessorConfig processorConfig;

    @Mock
    private PluginMetrics pluginMetrics;

    private CSVProcessor csvProcessor;

    @BeforeEach
    void setup() {
        CSVProcessorConfig defaultConfig = new CSVProcessorConfig();
        lenient().when(processorConfig.getSource()).thenReturn(defaultConfig.getSource());
        lenient().when(processorConfig.getDelimiter()).thenReturn(defaultConfig.getDelimiter());
        lenient().when(processorConfig.isDeleteHeader()).thenReturn(defaultConfig.isDeleteHeader());
        lenient().when(processorConfig.getQuoteCharacter()).thenReturn(defaultConfig.getQuoteCharacter());
        lenient().when(processorConfig.getColumnNamesSourceKey()).thenReturn(defaultConfig.getColumnNamesSourceKey());
        lenient().when(processorConfig.getColumnNames()).thenReturn(defaultConfig.getColumnNames());

        csvProcessor = createObjectUnderTest();
    }

    private CSVProcessor createObjectUnderTest() {
        return new CSVProcessor(pluginMetrics, processorConfig);
    }

    @Test
    public void test_when_messageIsEmpty_then_notParsed() {
        final Record<Event> eventUnderTest = createMessageEvent("");
        final List<Record<Event>> editedEvents = (List<Record<Event>>) csvProcessor.doExecute(Collections.singletonList(eventUnderTest));
        final Event parsedEvent = getSingleEvent(editedEvents);
        assertThat(parsedEvent.containsKey("message"), equalTo(true));
        assertThat(parsedEvent.containsKey("column1"), equalTo(false));
    }

    @Test
    public void test_when_delimiterIsTab_then_parsedCorrectly() {
        when(processorConfig.getDelimiter()).thenReturn("\t");
        csvProcessor = createObjectUnderTest();

        Record<Event> eventUnderTest = createMessageEvent("1\t2\t3");
        final List<Record<Event>> editedEvents = (List<Record<Event>>) csvProcessor.doExecute(Collections.singletonList(eventUnderTest));
        final Event parsedEvent = getSingleEvent(editedEvents);
        assertThatKeyEquals(parsedEvent, "column1", "1");
        assertThatKeyEquals(parsedEvent, "column2", "2");
        assertThatKeyEquals(parsedEvent, "column3", "3");
    }

    @Test
    public void test_when_deleteHeaderAndHeaderSourceDefined_then_headerIsDeleted() {
        when(processorConfig.isDeleteHeader()).thenReturn(true);
        when(processorConfig.getColumnNamesSourceKey()).thenReturn("header");
        csvProcessor = createObjectUnderTest();

        final Map<String, Object> eventData = new HashMap();
        eventData.put("message","1,2,3");
        eventData.put("header","col1,col2,col3");
        Record<Event> eventUnderTest = buildRecordWithEvent(eventData);
        final List<Record<Event>> editedEvents = (List<Record<Event>>) csvProcessor.doExecute(Collections.singletonList(eventUnderTest));
        final Event parsedEvent = getSingleEvent(editedEvents);

        assertThat(parsedEvent.containsKey("message"), equalTo(true));
        assertThat(parsedEvent.containsKey("header"), equalTo(false));

        assertThatKeyEquals(parsedEvent, "col1", "1");
        assertThatKeyEquals(parsedEvent, "col2", "2");
        assertThatKeyEquals(parsedEvent, "col3", "3");
    }

    @Test
    public void test_when_headerSource_then_usesHeaderSourceForParsing() {
        when(processorConfig.isDeleteHeader()).thenReturn(false);
        when(processorConfig.getColumnNamesSourceKey()).thenReturn("header");
        csvProcessor = createObjectUnderTest();

        final Map<String, Object> eventData = new HashMap();
        eventData.put("message","1,2,3");
        eventData.put("header","col1,col2,col3");
        Record<Event> eventUnderTest = buildRecordWithEvent(eventData);
        final List<Record<Event>> editedEvents = (List<Record<Event>>) csvProcessor.doExecute(Collections.singletonList(eventUnderTest));
        final Event parsedEvent = getSingleEvent(editedEvents);

        assertThat(parsedEvent.containsKey("message"), equalTo(true));
        assertThat(parsedEvent.containsKey("header"), equalTo(true));

        assertThatKeyEquals(parsedEvent, "col1", "1");
        assertThatKeyEquals(parsedEvent, "col2", "2");
        assertThatKeyEquals(parsedEvent, "col3", "3");
    }

    @Test
    public void test_when_deleteHeaderandNoHeaderSource_then_extraFieldsNotDeleted() {
        final Map<String, Object> eventData = new HashMap();
        eventData.put("message","1,2,3");
        eventData.put("header","col1,col2,col3");
        Record<Event> eventUnderTest = buildRecordWithEvent(eventData);
        final List<Record<Event>> editedEvents = (List<Record<Event>>) csvProcessor.doExecute(Collections.singletonList(eventUnderTest));
        final Event parsedEvent = getSingleEvent(editedEvents);

        assertThat(parsedEvent.containsKey("message"), equalTo(true));
        assertThat(parsedEvent.containsKey("header"), equalTo(true));

        assertThatKeyEquals(parsedEvent, "column1", "1");
        assertThatKeyEquals(parsedEvent, "column2", "2");
        assertThatKeyEquals(parsedEvent, "column3", "3");
    }

    @Test
    public void test_when_messageHasOneColumn_then_parsed() {
        final Record<Event> eventUnderTest = createMessageEvent("1");
        final List<Record<Event>> editedEvents = (List<Record<Event>>) csvProcessor.doExecute(Collections.singletonList(eventUnderTest));
        final Event parsedEvent = getSingleEvent(editedEvents);
        assertThat(parsedEvent.containsKey("message"), equalTo(true));
        assertThat(parsedEvent.containsKey("column1"), equalTo(true));
        assertThat(parsedEvent.containsKey("column2"), equalTo(false));
    }

    @Test
    public void test_when_tooManyUserSpecifiedColumns_then_ommitsExtraNames() {
        final List<String> columnNames = Arrays.asList("col1","col2","col3","col4");
        when(processorConfig.getColumnNames()).thenReturn(columnNames);
        csvProcessor = createObjectUnderTest();
        final Record<Event> eventUnderTest = createMessageEvent("1,2,3");
        final List<Record<Event>> editedEvents = (List<Record<Event>>) csvProcessor.doExecute(Collections.singletonList(eventUnderTest));
        final Event parsedEvent = getSingleEvent(editedEvents);
        assertThat(parsedEvent.containsKey("message"), equalTo(true));
        assertThat(parsedEvent.containsKey("col4"), equalTo(false));

        assertThatKeyEquals(parsedEvent, "col1", "1");
        assertThatKeyEquals(parsedEvent, "col2", "2");
        assertThatKeyEquals(parsedEvent, "col3", "3");
    }

    @Test
    public void test_when_notEnoughUserSpecifiedColumns_then_generatesExtraNames() {
        final List<String> columnNames = Arrays.asList("col1","col2");
        when(processorConfig.getColumnNames()).thenReturn(columnNames);
        csvProcessor = createObjectUnderTest();
        final Record<Event> eventUnderTest = createMessageEvent("1,2,3");
        final List<Record<Event>> editedEvents = (List<Record<Event>>) csvProcessor.doExecute(Collections.singletonList(eventUnderTest));
        final Event parsedEvent = getSingleEvent(editedEvents);
        assertThat(parsedEvent.containsKey("message"), equalTo(true));
        assertThat(parsedEvent.containsKey("column1"), equalTo(false));
        assertThat(parsedEvent.containsKey("column2"), equalTo(false));

        assertThatKeyEquals(parsedEvent, "col1", "1");
        assertThatKeyEquals(parsedEvent, "col2", "2");
        assertThatKeyEquals(parsedEvent, "column3", "3");
    }
    @Test
    public void test_when_tooManyHeaderSourceColumns_then_ommitsExtraNames() {
        when(processorConfig.getColumnNamesSourceKey()).thenReturn("header");
        csvProcessor = createObjectUnderTest();

        final Map<String, Object> eventData = new HashMap();
        eventData.put("message","1,2,3");
        eventData.put("header","col1,col2,col3,col4");
        final Record<Event> eventUnderTest = buildRecordWithEvent(eventData);
        final List<Record<Event>> editedEvents = (List<Record<Event>>) csvProcessor.doExecute(Collections.singletonList(eventUnderTest));
        final Event parsedEvent = getSingleEvent(editedEvents);
        assertThat(parsedEvent.containsKey("message"), equalTo(true));
        assertThat(parsedEvent.containsKey("col4"), equalTo(false));

        assertThatKeyEquals(parsedEvent, "col1", "1");
        assertThatKeyEquals(parsedEvent, "col2", "2");
        assertThatKeyEquals(parsedEvent, "col3", "3");
    }

    @Test
    public void test_when_notEnoughHeaderSourceColumns_then_generatesExtraNames() {
        when(processorConfig.getColumnNamesSourceKey()).thenReturn("header");
        csvProcessor = createObjectUnderTest();

        final Map<String, Object> eventData = new HashMap();
        eventData.put("message","1,2,3");
        eventData.put("header","col1,col2");
        final Record<Event> eventUnderTest = buildRecordWithEvent(eventData);
        final List<Record<Event>> editedEvents = (List<Record<Event>>) csvProcessor.doExecute(Collections.singletonList(eventUnderTest));
        final Event parsedEvent = getSingleEvent(editedEvents);
        assertThat(parsedEvent.containsKey("message"), equalTo(true));
        assertThat(parsedEvent.containsKey("column1"), equalTo(false));
        assertThat(parsedEvent.containsKey("column2"), equalTo(false));

        assertThatKeyEquals(parsedEvent, "col1", "1");
        assertThatKeyEquals(parsedEvent, "col2", "2");
        assertThatKeyEquals(parsedEvent, "column3", "3");
    }

    @Test
    public void test_when_twoEventsAndNoHeaderSourceOnOne_then_ChooseUserColumns() {
        when(processorConfig.getColumnNamesSourceKey()).thenReturn("header");
        final List<String> columnNames = Arrays.asList("user_col1","user_col2","user_col3");
        when(processorConfig.getColumnNames()).thenReturn(columnNames);
        csvProcessor = createObjectUnderTest();

        final Map<String, Object> firstEventData = new HashMap();
        firstEventData.put("message","1,2,3");
        firstEventData.put("header","col1,col2,col3");
        final Record<Event> firstEventUnderTest = buildRecordWithEvent(firstEventData);

        final Record<Event> secondEventUnderTest = createMessageEvent("1,2,3");

        final List<Record<Event>> editedEvents = (List<Record<Event>>) csvProcessor.doExecute(Arrays.asList(
                firstEventUnderTest, secondEventUnderTest));

        final Event firstParsedEvent = editedEvents.get(0).getData();
        final Event secondParsedEvent = editedEvents.get(1).getData();

        assertThatKeyEquals(firstParsedEvent, "col1", "1");
        assertThatKeyEquals(firstParsedEvent, "col2", "2");
        assertThatKeyEquals(firstParsedEvent, "col3", "3");

        assertThatKeyEquals(secondParsedEvent, "user_col1", "1");
        assertThatKeyEquals(secondParsedEvent, "user_col2", "2");
        assertThatKeyEquals(secondParsedEvent, "user_col3", "3");
    }

    @Test
    public void test_when_differentQuoteCharacter_then_parsesCorrectly() {
        when(processorConfig.getQuoteCharacter()).thenReturn("\'");
        csvProcessor = createObjectUnderTest();

        final Record<Event> eventUnderTest = createMessageEvent("'1','2','3'");
        final List<Record<Event>> editedEvents = (List<Record<Event>>) csvProcessor.doExecute(Collections.singletonList(eventUnderTest));
        final Event parsedEvent = getSingleEvent(editedEvents);

        assertThatKeyEquals(parsedEvent, "column1", "1");
        assertThatKeyEquals(parsedEvent, "column2", "2");
        assertThatKeyEquals(parsedEvent, "column3", "3");
    }


    @Test
    public void test_when_sourceIsNotMessage_then_parsesCorrectly() {
        when(processorConfig.getSource()).thenReturn("different_source");
        csvProcessor = createObjectUnderTest();

        final Map<String, Object> eventData = new HashMap();
        eventData.put("different_source","1,2,3");
        final Record<Event> eventUnderTest = buildRecordWithEvent(eventData);

        final List<Record<Event>> editedEvents = (List<Record<Event>>) csvProcessor.doExecute(Collections.singletonList(eventUnderTest));
        final Event parsedEvent = getSingleEvent(editedEvents);
        assertThat(parsedEvent.containsKey("different_source"), equalTo(true));
        assertThatKeyEquals(parsedEvent, "column1", "1");
        assertThatKeyEquals(parsedEvent, "column2", "2");
        assertThatKeyEquals(parsedEvent, "column3", "3");
    }

    private Record<Event> createMessageEvent(final String message) {
        final Map<String, Object> eventData = new HashMap();
        eventData.put("message",message);
        return buildRecordWithEvent(eventData);
    }

    private Record<Event> buildRecordWithEvent(final Map<String, Object> data) {
        return new Record<>(JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build());
    }

    private void assertThatKeyEquals(Event parsedEvent, final String key, final Object value) {
        assertThat(parsedEvent.containsKey(key), equalTo(true));
        assertThat(parsedEvent.get(key, String.class), equalTo(value));
    }

    private Event getSingleEvent(List<Record<Event>> editedRecords) {
        return editedRecords.get(0).getData();
    }
}