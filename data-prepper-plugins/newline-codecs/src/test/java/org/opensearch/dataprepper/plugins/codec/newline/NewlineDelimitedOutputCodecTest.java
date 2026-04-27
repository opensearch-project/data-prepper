/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
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
import java.util.UUID;

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
        final String message = UUID.randomUUID().toString();
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final OutputCodecContext codecContext = new OutputCodecContext();

        final Map<String, Object> eventData = new HashMap<>();
        eventData.put("message", message);
        final Event event = eventFactory.eventBuilder(EventBuilder.class).withData(eventData).build();

        codec.start(outputStream, event, codecContext);
        codec.writeEvent(event, outputStream);
        codec.complete(outputStream);

        final String output = outputStream.toString(StandardCharsets.UTF_8);
        assertThat(output, equalTo(message + System.lineSeparator()));
    }

    @Test
    void writeEvent_writes_nothing_when_message_is_missing_and_include_empty_objects_false() throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final OutputCodecContext codecContext = new OutputCodecContext();

        final Map<String, Object> eventData = new HashMap<>();
        eventData.put("other_field", "some value");
        final Event event = eventFactory.eventBuilder(EventBuilder.class).withData(eventData).build();

        codec.start(outputStream, event, codecContext);
        codec.writeEvent(event, outputStream);
        codec.complete(outputStream);

        final String output = outputStream.toString(StandardCharsets.UTF_8);
        assertThat(output, equalTo(""));
    }

    @Test
    void writeEvent_writes_nothing_when_message_is_null_and_include_empty_objects_false() throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final OutputCodecContext codecContext = new OutputCodecContext();

        final Map<String, Object> eventData = new HashMap<>();
        eventData.put("message", null);
        final Event event = eventFactory.eventBuilder(EventBuilder.class).withData(eventData).build();

        codec.start(outputStream, event, codecContext);
        codec.writeEvent(event, outputStream);
        codec.complete(outputStream);

        final String output = outputStream.toString(StandardCharsets.UTF_8);
        assertThat(output, equalTo(""));
    }

    @Test
    void writeEvent_writes_empty_line_when_message_is_missing_and_include_empty_objects_true() throws IOException {
        config.setIncludeEmptyObjects(true);
        codec = new NewlineDelimitedOutputCodec(config);

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
    void writeEvent_writes_empty_line_when_message_is_null_and_include_empty_objects_true() throws IOException {
        config.setIncludeEmptyObjects(true);
        codec = new NewlineDelimitedOutputCodec(config);

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