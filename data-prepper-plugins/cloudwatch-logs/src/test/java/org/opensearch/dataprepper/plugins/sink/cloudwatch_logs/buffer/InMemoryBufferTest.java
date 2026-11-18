/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.buffer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.DefaultEventHandle;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;

import java.time.Instant;
import java.util.ArrayList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class InMemoryBufferTest {
    private static InMemoryBuffer inMemoryBuffer;
    public static final String TEST_EVENT_MESSAGE = "testing";
    public static final int TEST_COLLECTION_SIZE = 3;
    private static EventHandle eventHandle;

    @BeforeEach
    void setUp() {
        inMemoryBuffer = new InMemoryBuffer();
        eventHandle = new DefaultEventHandle(Instant.now());
    }

    ArrayList<Record<Event>> getTestCollection() {
        ArrayList<Record<Event>> testCollection = new ArrayList<>();

        for (int i = 0; i < TEST_COLLECTION_SIZE; i++) {
            testCollection.add(new Record<>(JacksonEvent.fromMessage(TEST_EVENT_MESSAGE)));
        }

        return testCollection;
    }

    int getStringJsonMessageSize() {
        return JacksonEvent.fromMessage(TEST_EVENT_MESSAGE).toJsonString().length();
    }

    @Test
    void GIVEN_empty_buffer_WHEN_filled_SHOULD_return_valid_buffer_size_and_event_count() {
        assertThat(inMemoryBuffer.getBufferSize(), equalTo(0));
        assertThat(inMemoryBuffer.getEventCount(), equalTo(0));

        for (Record<Event> eventToTest: getTestCollection()) {
            inMemoryBuffer.writeEvent(eventHandle, eventToTest.getData().toJsonString().getBytes());
        }

        assertThat(inMemoryBuffer.getBufferSize(), equalTo(getStringJsonMessageSize() * TEST_COLLECTION_SIZE));
        assertThat(inMemoryBuffer.getEventCount(), equalTo(TEST_COLLECTION_SIZE));
    }

}
