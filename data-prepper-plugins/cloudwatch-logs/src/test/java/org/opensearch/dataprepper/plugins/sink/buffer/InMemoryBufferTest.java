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

    @Test
    void check_empty_buffer() {
        assertThat(inMemoryBuffer.getBufferSize(), equalTo(0));
        assertThat(inMemoryBuffer.getEventCount(), equalTo(0));
    }

    @Test
    void check_buffer_has_right_number_of_events_test() {
        for (Record<Event> eventToTest: getTestCollection()) {
            inMemoryBuffer.writeEvent(eventToTest);
        }

        assertThat(inMemoryBuffer.getEventCount(), equalTo(3));
    }

    @Test
    void check_buffer_has_right_size_test() {
        for (Record<Event> eventToTest: getTestCollection()) {
            inMemoryBuffer.writeEvent(eventToTest);
        }

        assertThat(inMemoryBuffer.getBufferSize(), equalTo(63));
    }

    //TODO: Add tests for getting events.
}
