/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.processor.csv;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CsvProcessorTest {
    @Mock
    private CsvProcessorConfig processorConfig;
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private Counter csvInvalidEventsCounter;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    private CsvProcessor csvProcessor;

    @BeforeEach
    void setup() {
        CsvProcessorConfig defaultConfig = new CsvProcessorConfig();
        lenient().when(processorConfig.getSource()).thenReturn(defaultConfig.getSource());
        lenient().when(processorConfig.getDelimiter()).thenReturn(defaultConfig.getDelimiter());
        lenient().when(processorConfig.isDeleteHeader()).thenReturn(defaultConfig.isDeleteHeader());
        lenient().when(processorConfig.getQuoteCharacter()).thenReturn(defaultConfig.getQuoteCharacter());
        lenient().when(processorConfig.getColumnNamesSourceKey()).thenReturn(defaultConfig.getColumnNamesSourceKey());
        lenient().when(processorConfig.getColumnNames()).thenReturn(defaultConfig.getColumnNames());
        lenient().when(processorConfig.isDeleteSource()).thenReturn(false);

        lenient().when(pluginMetrics.counter(CsvProcessor.CSV_INVALID_EVENTS)).thenReturn(csvInvalidEventsCounter);

        csvProcessor = createObjectUnderTest();
    }

    private CsvProcessor createObjectUnderTest() {
        return new CsvProcessor(pluginMetrics, processorConfig, expressionEvaluator);
    }

    @Test
    void delete_source_true_deletes_the_source() {
        when(processorConfig.isDeleteSource()).thenReturn(true);

        when(processorConfig.getSource()).thenReturn("different_source");

        final Map<String, Object> eventData = new HashMap<>();
        eventData.put("different_source","1,2,3");
        final Record<Event> eventUnderTest = buildRecordWithEvent(eventData);

        final List<Record<Event>> editedEvents = (List<Record<Event>>) csvProcessor.doExecute(Collections.singletonList(eventUnderTest));
        final Event parsedEvent = getSingleEvent(editedEvents);
        assertThat(parsedEvent.containsKey("different_source"), equalTo(false));
        assertThatKeyEquals(parsedEvent, "column1", "1");
        assertThatKeyEquals(parsedEvent, "column2", "2");
        assertThatKeyEquals(parsedEvent, "column3", "3");
    }

    @Test
    void do_nothing_when_source_is_null_value_or_does_not_exist_in_the_Event() {

        final Record<Event> eventUnderTest = createMessageEvent("");

        final String csvWhen = UUID.randomUUID().toString();
        when(processorConfig.getCsvWhen()).thenReturn(csvWhen);
        when(expressionEvaluator.isValidExpressionStatement(csvWhen)).thenReturn(true);
        when(expressionEvaluator.evaluateConditional(csvWhen, eventUnderTest.getData())).thenReturn(true);
        when(processorConfig.getSource()).thenReturn(UUID.randomUUID().toString());

        csvProcessor = createObjectUnderTest();


        final List<Record<Event>> editedEvents = (List<Record<Event>>) csvProcessor.doExecute(Collections.singletonList(eventUnderTest));
        final Event parsedEvent = getSingleEvent(editedEvents);

        assertThat(parsedEvent, equalTo(eventUnderTest.getData()));
    }

    @Test
    void test_when_messageIsEmpty_then_notParsed() {
        final Record<Event> eventUnderTest = createMessageEvent("");
        final List<Record<Event>> editedEvents = (List<Record<Event>>) csvProcessor.doExecute(Collections.singletonList(eventUnderTest));
        final Event parsedEvent = getSingleEvent(editedEvents);
        assertThat(parsedEvent.containsKey("message"), equalTo(true));
        assertThat(parsedEvent.containsKey("column1"), equalTo(false));
    }

    @Test
    void test_when_delimiterIsTab_then_parsedCorrectly() {
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
    void test_when_multLine_then_parsedCorrectly() {
        when(processorConfig.isMultiLine()).thenReturn(true);
        when(processorConfig.getDelimiter()).thenReturn(",");
        csvProcessor = createObjectUnderTest();

        Record<Event> eventUnderTest = createMessageEvent("key1,key2,key3\n1,2,3");
        final List<Record<Event>> editedEvents = (List<Record<Event>>) csvProcessor.doExecute(Collections.singletonList(eventUnderTest));
        final Event parsedEvent = getSingleEvent(editedEvents);
        assertThatKeyEquals(parsedEvent, "key1", "1");
        assertThatKeyEquals(parsedEvent, "key2", "2");
        assertThatKeyEquals(parsedEvent, "key3", "3");
    }

    @Test
    void test_when_multLine_invalid_keys_then_parsedCorrectly() {
        when(processorConfig.isMultiLine()).thenReturn(true);
        when(processorConfig.getDelimiter()).thenReturn(",");
        when(processorConfig.getNormalizeKeys()).thenReturn(true);
        csvProcessor = createObjectUnderTest();

        Record<Event> eventUnderTest = createMessageEvent("key%1,key&2,key^3\n1,2,3");
        final List<Record<Event>> editedEvents = (List<Record<Event>>) csvProcessor.doExecute(Collections.singletonList(eventUnderTest));
        final Event parsedEvent = getSingleEvent(editedEvents);
        // % is valid in event keys, so key%1 is not normalized; & and ^ are invalid and replaced with _
        assertThatKeyEquals(parsedEvent, "key%1", "1");
        assertThatKeyEquals(parsedEvent, "key_2", "2");
        assertThatKeyEquals(parsedEvent, "key_3", "3");
    }

    @Test
    void test_when_deleteHeaderAndHeaderSourceDefined_then_headerIsDeleted() {
        when(processorConfig.isDeleteHeader()).thenReturn(true);
        when(processorConfig.getColumnNamesSourceKey()).thenReturn("header");

        final Map<String, Object> eventData = new HashMap<>();
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
    void test_when_ColumnNames_with_invalid_keys_parsedCorrectly() {
        when(processorConfig.getColumnNamesSourceKey()).thenReturn("header");
        when(processorConfig.getNormalizeKeys()).thenReturn(true);
        csvProcessor = createObjectUnderTest();

        final Map<String, Object> eventData = new HashMap<>();
        eventData.put("message","1,2,3");
        eventData.put("header","col%1,col&2,col^3");
        Record<Event> eventUnderTest = buildRecordWithEvent(eventData);
        final List<Record<Event>> editedEvents = (List<Record<Event>>) csvProcessor.doExecute(Collections.singletonList(eventUnderTest));
        final Event parsedEvent = getSingleEvent(editedEvents);

        assertThat(parsedEvent.containsKey("message"), equalTo(true));

        // % is valid in event keys, so col%1 is not normalized; & and ^ are invalid and replaced with _
        assertThatKeyEquals(parsedEvent, "col%1", "1");
        assertThatKeyEquals(parsedEvent, "col_2", "2");
        assertThatKeyEquals(parsedEvent, "col_3", "3");
    }

    @Test
    void test_when_headerSource_then_usesHeaderSourceForParsing() {
        when(processorConfig.isDeleteHeader()).thenReturn(false);
        when(processorConfig.getColumnNamesSourceKey()).thenReturn("header");

        final Map<String, Object> eventData = new HashMap<>();
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
    void test_when_deleteHeaderAndNoHeaderSource_then_extraFieldsNotDeleted() {
        final Map<String, Object> eventData = new HashMap<>();
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
    void test_when_messageHasOneColumn_then_parsed() {
        final Record<Event> eventUnderTest = createMessageEvent("1");
        final List<Record<Event>> editedEvents = (List<Record<Event>>) csvProcessor.doExecute(Collections.singletonList(eventUnderTest));
        final Event parsedEvent = getSingleEvent(editedEvents);
        assertThat(parsedEvent.containsKey("message"), equalTo(true));
        assertThat(parsedEvent.containsKey("column1"), equalTo(true));
        assertThat(parsedEvent.containsKey("column2"), equalTo(false));
    }

    @Test
    void test_when_tooManyUserSpecifiedColumns_then_omitsExtraNames() {
        final List<String> columnNames = Arrays.asList("col1","col2","col3","col4");
        when(processorConfig.getColumnNames()).thenReturn(columnNames);

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
    void test_when_notEnoughUserSpecifiedColumns_then_generatesExtraNames() {
        final List<String> columnNames = Arrays.asList("col1","col2");
        when(processorConfig.getColumnNames()).thenReturn(columnNames);

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
    void test_when_tooManyHeaderSourceColumns_then_omitsExtraNames() {
        when(processorConfig.getColumnNamesSourceKey()).thenReturn("header");

        final Map<String, Object> eventData = new HashMap<>();
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
    void test_when_notEnoughHeaderSourceColumns_then_generatesExtraNames() {
        when(processorConfig.getColumnNamesSourceKey()).thenReturn("header");

        final Map<String, Object> eventData = new HashMap<>();
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
    void test_when_twoEventsAndNoHeaderSourceOnOne_then_ChooseUserColumns() {
        when(processorConfig.getColumnNamesSourceKey()).thenReturn("header");
        final List<String> columnNames = Arrays.asList("user_col1","user_col2","user_col3");
        when(processorConfig.getColumnNames()).thenReturn(columnNames);

        final Map<String, Object> firstEventData = new HashMap<>();
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
    void test_when_differentQuoteCharacter_then_parsesCorrectly() {
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
    void test_when_sourceIsNotMessage_then_parsesCorrectly() {
        when(processorConfig.getSource()).thenReturn("different_source");

        final Map<String, Object> eventData = new HashMap<>();
        eventData.put("different_source","1,2,3");
        final Record<Event> eventUnderTest = buildRecordWithEvent(eventData);

        final List<Record<Event>> editedEvents = (List<Record<Event>>) csvProcessor.doExecute(Collections.singletonList(eventUnderTest));
        final Event parsedEvent = getSingleEvent(editedEvents);
        assertThat(parsedEvent.containsKey("different_source"), equalTo(true));
        assertThatKeyEquals(parsedEvent, "column1", "1");
        assertThatKeyEquals(parsedEvent, "column2", "2");
        assertThatKeyEquals(parsedEvent, "column3", "3");
    }

    @Test
    void test_when_multilineInput_then_parseBehavior() {
        when(processorConfig.getColumnNamesSourceKey()).thenReturn("header");

        final Map<String, Object> eventData = new HashMap<>();
        eventData.put("message","1,2,3\n4,5,6");
        eventData.put("header","col1,col2");
        final Record<Event> eventUnderTest = buildRecordWithEvent(eventData);
        final List<Record<Event>> editedEvents = (List<Record<Event>>) csvProcessor.doExecute(Collections.singletonList(eventUnderTest));

        verifyNoInteractions(csvInvalidEventsCounter);

        final Event parsedEvent = getSingleEvent(editedEvents);

        assertThat(parsedEvent.containsKey("message"), equalTo(true));
        assertThat(parsedEvent.containsKey("column1"), equalTo(false));
        assertThat(parsedEvent.containsKey("column2"), equalTo(false));

        assertThatKeyEquals(parsedEvent, "col1", "1");
        assertThatKeyEquals(parsedEvent, "col2", "2");
        assertThatKeyEquals(parsedEvent, "column3", "3");
    }

    @Test
    void test_when_invalidEvent_then_metricIncrementedAndNoException() {
        final Map<String, Object> eventData = new HashMap<>();
        eventData.put("message","1,2,\"3");

        final Record<Event> eventUnderTest = buildRecordWithEvent(eventData);
        final List<Record<Event>> editedEvents = (List<Record<Event>>) csvProcessor.doExecute(Collections.singletonList(eventUnderTest));

        verify(csvInvalidEventsCounter).increment();

        final Event parsedEvent = getSingleEvent(editedEvents);

        assertThat(parsedEvent.containsKey("message"), equalTo(true));
        assertThat(parsedEvent.containsKey("column1"), equalTo(false));
        assertThat(parsedEvent.containsKey("column2"), equalTo(false));
        assertThat(parsedEvent.containsKey("column3"), equalTo(false));
    }

    @Test
    void invalid_csv_when_throws_InvalidPluginConfigurationException() {
        final String csvWhen = UUID.randomUUID().toString();

        when(processorConfig.getCsvWhen()).thenReturn(csvWhen);
        when(expressionEvaluator.isValidExpressionStatement(csvWhen)).thenReturn(false);

        assertThrows(InvalidPluginConfigurationException.class, this::createObjectUnderTest);
    }

    @Test
    void do_nothing_when_expression_evaluation_returns_false_for_event() {
        final String csvWhen = UUID.randomUUID().toString();

        when(processorConfig.getCsvWhen()).thenReturn(csvWhen);
        when(expressionEvaluator.isValidExpressionStatement(csvWhen)).thenReturn(true);

        final Record<Event> eventUnderTest = createMessageEvent("");
        when(expressionEvaluator.evaluateConditional(csvWhen, eventUnderTest.getData())).thenReturn(false);

        csvProcessor = createObjectUnderTest();


        final List<Record<Event>> editedEvents = (List<Record<Event>>) csvProcessor.doExecute(Collections.singletonList(eventUnderTest));
        final Event parsedEvent = getSingleEvent(editedEvents);

        assertThat(parsedEvent, equalTo(eventUnderTest.getData()));
    }

    private Record<Event> createMessageEvent(final String message) {
        final Map<String, Object> eventData = new HashMap<>();
        eventData.put("message",message);
        return buildRecordWithEvent(eventData);
    }

    private Record<Event> buildRecordWithEvent(final Map<String, Object> data) {
        return new Record<>(JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build());
    }

    private void assertThatKeyEquals(final Event parsedEvent, final String key, final Object value) {
        assertThat(parsedEvent.containsKey(key), equalTo(true));
        assertThat(parsedEvent.get(key, String.class), equalTo(value));
    }

    private Event getSingleEvent(final List<Record<Event>> editedRecords) {
        return editedRecords.get(0).getData();
    }
}
