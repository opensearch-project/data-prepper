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

    @BeforeEach
    void setUp() {
        inMemoryBuffer = new InMemoryBuffer();
    }

    ArrayList<Record<Event>> getTestCollection() {
        ArrayList<Record<Event>> testCollection = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            testCollection.add(new Record<>(JacksonEvent.fromMessage("testing")));
        }

        return testCollection;
    }

    String getStringJsonMessage() {
        return JacksonEvent.fromMessage("testing").toJsonString();
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

        assertThat(inMemoryBuffer.getEventCount(), equalTo(3));
    }

    @Test
    void check_right_event_count_after_event_fetch_test() {
        for (Record<Event> eventToTest: getTestCollection()) {
            inMemoryBuffer.writeEvent(eventToTest.getData().toJsonString().getBytes());
        }

        inMemoryBuffer.popEvent();

        assertThat(inMemoryBuffer.getEventCount(), equalTo(2));
    }

    @Test
    void check_right_buffer_size_after_event_fetch_test() {
        for (Record<Event> eventToTest: getTestCollection()) {
            inMemoryBuffer.writeEvent(eventToTest.getData().toJsonString().getBytes());
        }

        inMemoryBuffer.popEvent();

        assertThat(inMemoryBuffer.getBufferSize(), equalTo(42));
    }

    @Test
    void check_buffer_has_right_size_test() {
        for (Record<Event> eventToTest: getTestCollection()) {
            inMemoryBuffer.writeEvent(eventToTest.getData().toJsonString().getBytes());
        }

        assertThat(inMemoryBuffer.getBufferSize(), equalTo(63));
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
