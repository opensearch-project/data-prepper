/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StringProcessorTests {

    private final String TEST_EVENT_TYPE = "test_event_type";
    private final String TEST_KEY = "test_key";
    private final String UPPERCASE_TEST_STRING = "data_prepper";
    private final String LOWERCASE_TEST_STRING = "STRING_CONVERTER";
    private final Record<Event> TEST_RECORD_1 = new Record<>(
            JacksonEvent.builder()
                    .withEventType(TEST_EVENT_TYPE)
                    .withData(Map.of(TEST_KEY, UPPERCASE_TEST_STRING))
                    .build()
    );
    private final Record<Event> TEST_RECORD_2 = new Record<>(
            JacksonEvent.builder()
                    .withEventType(TEST_EVENT_TYPE)
                    .withData(Map.of(TEST_KEY, LOWERCASE_TEST_STRING))
                    .build()
    );
    private final List<Record<Event>> TEST_RECORDS = Arrays.asList(TEST_RECORD_1, TEST_RECORD_2);
    private StringProcessor.Configuration configuration;

    @BeforeEach
    void setUp() {
        configuration = new StringProcessor.Configuration();
    }

    private StringProcessor createObjectUnderTest() {
        return new StringProcessor(configuration);
    }

    @Test
    public void testStringPrepperDefault() {

        final StringProcessor stringProcessor = createObjectUnderTest();
        final List<Record<Event>> modifiedRecords = (List<Record<Event>>) stringProcessor.execute(TEST_RECORDS);
        stringProcessor.shutdown();

        final List<Event> modifiedRecordEvents = modifiedRecords.stream().map(Record::getData).collect(Collectors.toList());

        assertThat(modifiedRecordEvents.size(), equalTo(2));
        final Event firstEvent = modifiedRecordEvents.get(0);
        final Event secondEvent = modifiedRecordEvents.get(1);
        assertTrue(firstEvent.containsKey(TEST_KEY));
        assertThat(firstEvent.getMetadata().getEventType(), equalTo(TEST_EVENT_TYPE));
        assertThat(firstEvent.get(TEST_KEY, String.class), equalTo(UPPERCASE_TEST_STRING.toUpperCase()));
        assertTrue(secondEvent.containsKey(TEST_KEY));
        assertThat(secondEvent.getMetadata().getEventType(), equalTo(TEST_EVENT_TYPE));
        assertThat(secondEvent.get(TEST_KEY, String.class), equalTo(LOWERCASE_TEST_STRING));
    }

    @Test
    public void testStringPrepperLowerCase() {
        configuration.setUpperCase(false);
        final StringProcessor stringProcessor = createObjectUnderTest();
        final List<Record<Event>> modifiedRecords = (List<Record<Event>>) stringProcessor.execute(TEST_RECORDS);
        stringProcessor.shutdown();

        final List<Event> modifiedRecordEvents = modifiedRecords.stream().map(Record::getData).collect(Collectors.toList());

        assertThat(modifiedRecordEvents.size(), equalTo(2));
        final Event firstEvent = modifiedRecordEvents.get(0);
        final Event secondEvent = modifiedRecordEvents.get(1);
        assertTrue(firstEvent.containsKey(TEST_KEY));
        assertThat(firstEvent.getMetadata().getEventType(), equalTo(TEST_EVENT_TYPE));
        assertThat(firstEvent.get(TEST_KEY, String.class), equalTo(UPPERCASE_TEST_STRING));
        assertTrue(secondEvent.containsKey(TEST_KEY));
        assertThat(secondEvent.getMetadata().getEventType(), equalTo(TEST_EVENT_TYPE));
        assertThat(secondEvent.get(TEST_KEY, String.class), equalTo(LOWERCASE_TEST_STRING.toLowerCase()));
    }

    @Test
    public void testStringPrepperUpperCase() {
        configuration.setUpperCase(true);
        final StringProcessor stringProcessor = createObjectUnderTest();
        final List<Record<Event>> modifiedRecords = (List<Record<Event>>) stringProcessor.execute(TEST_RECORDS);
        stringProcessor.shutdown();

        final List<Event> modifiedRecordEvents = modifiedRecords.stream().map(Record::getData).collect(Collectors.toList());

        assertThat(modifiedRecordEvents.size(), equalTo(2));
        final Event firstEvent = modifiedRecordEvents.get(0);
        final Event secondEvent = modifiedRecordEvents.get(1);
        assertTrue(firstEvent.containsKey(TEST_KEY));
        assertThat(firstEvent.getMetadata().getEventType(), equalTo(TEST_EVENT_TYPE));
        assertThat(firstEvent.get(TEST_KEY, String.class), equalTo(UPPERCASE_TEST_STRING.toUpperCase()));
        assertTrue(secondEvent.containsKey(TEST_KEY));
        assertThat(secondEvent.getMetadata().getEventType(), equalTo(TEST_EVENT_TYPE));
        assertThat(secondEvent.get(TEST_KEY, String.class), equalTo(LOWERCASE_TEST_STRING));
    }

    @Test
    public void testPrepareForShutdown() {
        final StringProcessor stringProcessor = createObjectUnderTest();

        stringProcessor.prepareForShutdown();

        assertThat(stringProcessor.isReadyForShutdown(), equalTo(true));
    }

}
