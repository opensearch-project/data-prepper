package com.amazon.dataprepper.model.event;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class EventTypeTest {

    @Test
    public void testEventTypeGetByName_log() {
        final EventType log = EventType.getByName("Log");
        assertThat(log, is(equalTo(EventType.LOG)));
        assertThat(log.toString(), is(equalTo("LOG")));
    }

    @Test
    public void testEventTypeGetByName_trace() {
        final EventType trace = EventType.getByName("trace");
        assertThat(trace, is(equalTo(EventType.TRACE)));
        assertThat(trace.toString(), is(equalTo("TRACE")));
    }
}
