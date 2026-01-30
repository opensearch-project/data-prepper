/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec.newline;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.event.TestEventFactory;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventBuilder;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NewlineDelimitedOutputCodecTest {

    private NewlineDelimitedOutputCodec codec;
    private NewlineDelimitedOutputConfig config;
    private EventFactory eventFactory;

    @BeforeEach
    void setUp() {
        config = new NewlineDelimitedOutputConfig();
        codec = new NewlineDelimitedOutputCodec(config);
        eventFactory = TestEventFactory.getTestEventFactory();
    }

    @Test
    void constructor_throws_if_config_is_null() {
        assertThrows(NullPointerException.class, () -> new NewlineDelimitedOutputCodec(null));
    }

    @Test
    void writeEvent_writes_message_field_as_plain_text() throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final OutputCodecContext codecContext = new OutputCodecContext();

        final Map<String, Object> eventData = new HashMap<>();
        eventData.put("message", "r_id=2bb2bd0aeece11f0bea286fb87d48915-ticket_update,tp=00-13a3bb055e6b589dbc0f952e0d75020a-1f2e986790c19742-01");
        final Event event = eventFactory.eventBuilder(EventBuilder.class).withData(eventData).build();

        codec.start(outputStream, event, codecContext);
        codec.writeEvent(event, outputStream);
        codec.complete(outputStream);

        final String output = outputStream.toString(StandardCharsets.UTF_8);
        assertThat(output, equalTo("r_id=2bb2bd0aeece11f0bea286fb87d48915-ticket_update,tp=00-13a3bb055e6b589dbc0f952e0d75020a-1f2e986790c19742-01" + System.lineSeparator()));
    }

    @Test
    void writeEvent_writes_empty_string_when_message_is_missing() throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final OutputCodecContext codecContext = new OutputCodecContext();

        final Map<String, Object> eventData = new HashMap<>();
        eventData.put("other_field", "some value");
        final Event event = eventFactory.eventBuilder(EventBuilder.class).withData(eventData).build();

        codec.start(outputStream, event, codecContext);
        codec.writeEvent(event, outputStream);
        codec.complete(outputStream);

        final String output = outputStream.toString(StandardCharsets.UTF_8);
        assertThat(output, equalTo("" + System.lineSeparator()));
    }

    @Test
    void writeEvent_writes_empty_string_when_message_is_null() throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final OutputCodecContext codecContext = new OutputCodecContext();

        final Map<String, Object> eventData = new HashMap<>();
        eventData.put("message", null);
        final Event event = eventFactory.eventBuilder(EventBuilder.class).withData(eventData).build();

        codec.start(outputStream, event, codecContext);
        codec.writeEvent(event, outputStream);
        codec.complete(outputStream);

        final String output = outputStream.toString(StandardCharsets.UTF_8);
        assertThat(output, equalTo("" + System.lineSeparator()));
    }

    @Test
    void writeEvent_writes_multiple_events_on_separate_lines() throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final OutputCodecContext codecContext = new OutputCodecContext();

        final Map<String, Object> eventData1 = new HashMap<>();
        eventData1.put("message", "First message");
        final Event event1 = eventFactory.eventBuilder(EventBuilder.class).withData(eventData1).build();

        final Map<String, Object> eventData2 = new HashMap<>();
        eventData2.put("message", "Second message");
        final Event event2 = eventFactory.eventBuilder(EventBuilder.class).withData(eventData2).build();

        codec.start(outputStream, event1, codecContext);
        codec.writeEvent(event1, outputStream);
        codec.writeEvent(event2, outputStream);
        codec.complete(outputStream);

        final String output = outputStream.toString(StandardCharsets.UTF_8);
        assertThat(output, equalTo("First message" + System.lineSeparator() + "Second message" + System.lineSeparator()));
    }

    @Test
    void getExtension_returns_txt() {
        assertThat(codec.getExtension(), equalTo("txt"));
    }

    @Test
    void createWriter_writes_message_field_as_plain_text() throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final OutputCodecContext codecContext = new OutputCodecContext();

        final Map<String, Object> eventData = new HashMap<>();
        eventData.put("message", "Test message content");
        final Event event = eventFactory.eventBuilder(EventBuilder.class).withData(eventData).build();

        final OutputCodec.Writer writer = codec.createWriter(outputStream, event, codecContext);
        writer.writeEvent(event);
        writer.complete();

        final String output = outputStream.toString(StandardCharsets.UTF_8);
        assertThat(output, equalTo("Test message content" + System.lineSeparator()));
    }
}