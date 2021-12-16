/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.prepper;

import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.JacksonEvent;
import com.amazon.dataprepper.model.record.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StringPrepperTests {

    private final String TEST_EVENT_TYPE = "test_event_type";
    private final String UPPERCASE_TEST_KEY = "test_key";
    private final String LOWERCASE_TEST_KEY = "TEST_KEY";
    private final String UPPERCASE_TEST_STRING = "data_prepper";
    private final String LOWERCASE_TEST_STRING = "STRING_CONVERTER";
    private final Record<Event> TEST_RECORD_1 = new Record<>(
            JacksonEvent.builder()
                    .withEventType(TEST_EVENT_TYPE)
                    .withData(Map.of(UPPERCASE_TEST_KEY, UPPERCASE_TEST_STRING))
                    .build()
    );
    private final Record<Event> TEST_RECORD_2 = new Record<>(
            JacksonEvent.builder()
                    .withEventType(TEST_EVENT_TYPE)
                    .withData(Map.of(LOWERCASE_TEST_KEY, LOWERCASE_TEST_STRING))
                    .build()
    );
    private final List<Record<Event>> TEST_RECORDS = Arrays.asList(TEST_RECORD_1, TEST_RECORD_2);
    private StringPrepper.Configuration configuration;

    @BeforeEach
    void setUp() {
        configuration = new StringPrepper.Configuration();
    }

    private StringPrepper createObjectUnderTest() {
        return new StringPrepper(configuration);
    }

    @Test
    public void testStringPrepperDefault() {

        final StringPrepper stringPrepper = createObjectUnderTest();
        final List<Record<Event>> modifiedRecords = (List<Record<Event>>) stringPrepper.execute(TEST_RECORDS);
        stringPrepper.shutdown();

        final List<Event> modifiedRecordEvents = modifiedRecords.stream().map(Record::getData).collect(Collectors.toList());

        assertThat(modifiedRecordEvents.size(), equalTo(2));
        final Event firstEvent = modifiedRecordEvents.get(0);
        final Event secondEvent = modifiedRecordEvents.get(1);
        assertTrue(firstEvent.containsKey(UPPERCASE_TEST_KEY.toUpperCase()));
        assertThat(firstEvent.getMetadata().getEventType(), equalTo(TEST_EVENT_TYPE));
        assertThat(firstEvent.get(UPPERCASE_TEST_KEY.toUpperCase(), String.class), equalTo(UPPERCASE_TEST_STRING.toUpperCase()));
        assertTrue(secondEvent.containsKey(LOWERCASE_TEST_KEY));
        assertThat(secondEvent.getMetadata().getEventType(), equalTo(TEST_EVENT_TYPE));
        assertThat(secondEvent.get(LOWERCASE_TEST_KEY, String.class), equalTo(LOWERCASE_TEST_STRING));
    }

    @Test
    public void testStringPrepperLowerCase() {
        configuration.setUpperCase(false);
        final StringPrepper stringPrepper = createObjectUnderTest();
        final List<Record<Event>> modifiedRecords = (List<Record<Event>>) stringPrepper.execute(TEST_RECORDS);
        stringPrepper.shutdown();

        final List<Event> modifiedRecordEvents = modifiedRecords.stream().map(Record::getData).collect(Collectors.toList());

        assertThat(modifiedRecordEvents.size(), equalTo(2));
        final Event firstEvent = modifiedRecordEvents.get(0);
        final Event secondEvent = modifiedRecordEvents.get(1);
        assertTrue(firstEvent.containsKey(UPPERCASE_TEST_KEY));
        assertThat(firstEvent.getMetadata().getEventType(), equalTo(TEST_EVENT_TYPE));
        assertThat(firstEvent.get(UPPERCASE_TEST_KEY, String.class), equalTo(UPPERCASE_TEST_STRING));
        assertTrue(secondEvent.containsKey(LOWERCASE_TEST_KEY.toLowerCase()));
        assertThat(secondEvent.getMetadata().getEventType(), equalTo(TEST_EVENT_TYPE));
        assertThat(secondEvent.get(LOWERCASE_TEST_KEY.toLowerCase(), String.class), equalTo(LOWERCASE_TEST_STRING.toLowerCase()));
    }

    @Test
    public void testStringPrepperUpperCase() {
        configuration.setUpperCase(true);
        final StringPrepper stringPrepper = createObjectUnderTest();
        final List<Record<Event>> modifiedRecords = (List<Record<Event>>) stringPrepper.execute(TEST_RECORDS);
        stringPrepper.shutdown();

        final List<Event> modifiedRecordEvents = modifiedRecords.stream().map(Record::getData).collect(Collectors.toList());

        assertThat(modifiedRecordEvents.size(), equalTo(2));
        final Event firstEvent = modifiedRecordEvents.get(0);
        final Event secondEvent = modifiedRecordEvents.get(1);
        assertTrue(firstEvent.containsKey(UPPERCASE_TEST_KEY.toUpperCase()));
        assertThat(firstEvent.getMetadata().getEventType(), equalTo(TEST_EVENT_TYPE));
        assertThat(firstEvent.get(UPPERCASE_TEST_KEY.toUpperCase(), String.class), equalTo(UPPERCASE_TEST_STRING.toUpperCase()));
        assertTrue(secondEvent.containsKey(LOWERCASE_TEST_KEY));
        assertThat(secondEvent.getMetadata().getEventType(), equalTo(TEST_EVENT_TYPE));
        assertThat(secondEvent.get(LOWERCASE_TEST_KEY, String.class), equalTo(LOWERCASE_TEST_STRING));
    }

    @Test
    public void testPrepareForShutdown() {
        final StringPrepper stringPrepper = createObjectUnderTest();

        stringPrepper.prepareForShutdown();

        assertThat(stringPrepper.isReadyForShutdown(), equalTo(true));
    }

}
