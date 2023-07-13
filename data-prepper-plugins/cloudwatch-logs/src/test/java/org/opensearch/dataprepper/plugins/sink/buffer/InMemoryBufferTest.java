/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.buffer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;

import java.util.ArrayList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class InMemoryBufferTest {
    private static InMemoryBuffer inMemoryBuffer;
    public static final String TEST_EVENT_MESSAGE = "testing";
    public static final int TEST_COLLECTION_SIZE = 3;

    @BeforeEach
    void setUp() {
        inMemoryBuffer = new InMemoryBuffer();
    }

    ArrayList<Record<Event>> getTestCollection() {
        ArrayList<Record<Event>> testCollection = new ArrayList<>();

        for (int i = 0; i < TEST_COLLECTION_SIZE; i++) {
            testCollection.add(new Record<>(JacksonEvent.fromMessage(TEST_EVENT_MESSAGE)));
        }

        return testCollection;
    }

    String getStringJsonMessage() {
        return JacksonEvent.fromMessage(TEST_EVENT_MESSAGE).toJsonString();
    }

    int getStringJsonMessageSize() {
        return JacksonEvent.fromMessage(TEST_EVENT_MESSAGE).toJsonString().length();
    }

    @Test
    void check_empty_buffer() {
        assertThat(inMemoryBuffer.getBufferSize(), equalTo(0));
        assertThat(inMemoryBuffer.getEventCount(), equalTo(0));
    }

    @Test
    void check_buffer_has_right_number_of_events_test() {
        for (Record<Event> eventToTest: getTestCollection()) {
            inMemoryBuffer.writeEvent(eventToTest.getData().toJsonString().getBytes());
        }

        assertThat(inMemoryBuffer.getEventCount(), equalTo(TEST_COLLECTION_SIZE));
    }

    @Test
    void check_right_event_count_after_event_fetch_test() {
        for (Record<Event> eventToTest: getTestCollection()) {
            inMemoryBuffer.writeEvent(eventToTest.getData().toJsonString().getBytes());
        }

        inMemoryBuffer.popEvent();

        assertThat(inMemoryBuffer.getEventCount(), equalTo(TEST_COLLECTION_SIZE - 1));
    }

    @Test
    void check_right_buffer_size_after_event_fetch_test() {
        for (Record<Event> eventToTest: getTestCollection()) {
            inMemoryBuffer.writeEvent(eventToTest.getData().toJsonString().getBytes());
        }

        inMemoryBuffer.popEvent();

        assertThat(inMemoryBuffer.getBufferSize(), equalTo(getStringJsonMessageSize() * (TEST_COLLECTION_SIZE - 1)));
    }

    @Test
    void check_buffer_has_right_size_test() {
        for (Record<Event> eventToTest: getTestCollection()) {
            inMemoryBuffer.writeEvent(eventToTest.getData().toJsonString().getBytes());
        }

        assertThat(inMemoryBuffer.getBufferSize(), equalTo(getStringJsonMessageSize() * TEST_COLLECTION_SIZE));
    }

    //TODO: Add tests for getting events.
    @Test
    void check_if_event_matches_test() {
        for (Record<Event> eventToTest: getTestCollection()) {
            inMemoryBuffer.writeEvent(eventToTest.getData().toJsonString().getBytes());
        }

        int eventCount = inMemoryBuffer.getEventCount();

        for (int i = 0; i < eventCount; i++) {
            assertThat(new String(inMemoryBuffer.popEvent()), equalTo(getStringJsonMessage()));
        }
    }
}
