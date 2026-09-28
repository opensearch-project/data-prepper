/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.dataprepper.plugins.codec.event_json;

import com.fasterxml.jackson.core.exc.StreamConstraintsException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;

import org.mockito.Mock;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.mockito.Mockito.verifyNoInteractions;

import org.opensearch.dataprepper.model.configuration.DataPrepperVersion;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;

import java.io.ByteArrayInputStream;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

public class EventJsonInputCodecTest {
    private static final Integer BYTEBUFFER_SIZE = 1024;
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    @Mock
    private EventJsonInputCodecConfig eventJsonInputCodecConfig;

    private EventJsonInputCodec inputCodec;
    private ByteArrayInputStream inputStream;

    @BeforeEach
    public void setup() {
        eventJsonInputCodecConfig = mock(EventJsonInputCodecConfig.class);
        when(eventJsonInputCodecConfig.getOverrideTimeReceived()).thenReturn(false);
    }

    public EventJsonInputCodec createInputCodec() {
        return new EventJsonInputCodec(eventJsonInputCodecConfig);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "{}"})
    public void emptyTest(String input) throws Exception {
        input = "{\"" + EventJsonDefines.VERSION + "\":\"" + DataPrepperVersion.getCurrentVersion().toString() + "\", \"" + EventJsonDefines.EVENTS + "\":[" + input + "]}";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(input.getBytes());
        inputCodec = createInputCodec();
        Consumer<Record<Event>> consumer = mock(Consumer.class);
        inputCodec.parse(inputStream, consumer);
        verifyNoInteractions(consumer);
    }

    @Test
    public void inCompatibleVersionTest() throws Exception {
        inputCodec = createInputCodec();
        final String key = UUID.randomUUID().toString();
        final String value = UUID.randomUUID().toString();
        Map<String, Object> data = Map.of(key, value);
        Instant startTime = Instant.now().truncatedTo(ChronoUnit.MICROS);
        Event event = createEvent(data, startTime);

        Map<String, Object> dataMap = event.toMap();
        Map<String, Object> metadataMap = objectMapper.convertValue(event.getMetadata(), Map.class);
        String input = "{\"" + EventJsonDefines.VERSION + "\":\"3.0\", \"" + EventJsonDefines.EVENTS + "\":[";
        String comma = "";
        for (int i = 0; i < 2; i++) {
            input += comma + "{\"data\":" + objectMapper.writeValueAsString(dataMap) + "," + "\"metadata\":" + objectMapper.writeValueAsString(metadataMap) + "}";
            comma = ",";
        }
        input += "]}";
        inputStream = new ByteArrayInputStream(input.getBytes());
        List<Record<Event>> records = new LinkedList<>();
        inputCodec.parse(inputStream, records::add);
        assertThat(records.size(), equalTo(0));
    }

    @Test
    public void basicTest() throws Exception {
        when(eventJsonInputCodecConfig.getOverrideTimeReceived()).thenReturn(true);
        inputCodec = createInputCodec();
        final String key = UUID.randomUUID().toString();
        final String value = UUID.randomUUID().toString();
        Map<String, Object> data = Map.of(key, value);
        Instant startTime = Instant.now().truncatedTo(ChronoUnit.MICROS);
        Event event = createEvent(data, startTime);

        Map<String, Object> dataMap = event.toMap();
        Map<String, Object> metadataMap = objectMapper.convertValue(event.getMetadata(), Map.class);
        String input = "{\"" + EventJsonDefines.VERSION + "\":\"" + DataPrepperVersion.getCurrentVersion().toString() + "\", \"" + EventJsonDefines.EVENTS + "\":[";
        String comma = "";
        for (int i = 0; i < 2; i++) {
            input += comma + "{\"data\":" + objectMapper.writeValueAsString(dataMap) + "," + "\"metadata\":" + objectMapper.writeValueAsString(metadataMap) + "}";
            comma = ",";
        }
        input += "]}";
        inputStream = new ByteArrayInputStream(input.getBytes());
        List<Record<Event>> records = new LinkedList<>();
        inputCodec.parse(inputStream, records::add);
        assertThat(records.size(), equalTo(2));
        for (Record record : records) {
            Event e = (Event) record.getData();
            assertThat(e.get(key, String.class), equalTo(value));
            assertThat(e.getMetadata().getTimeReceived(), equalTo(startTime));
            assertThat(e.getMetadata().getTags().size(), equalTo(0));
            assertThat(e.getMetadata().getExternalOriginationTime(), equalTo(null));
        }
    }

    @Test
    public void test_with_timeReceivedOverridden() throws Exception {
        inputCodec = createInputCodec();
        final String key = UUID.randomUUID().toString();
        final String value = UUID.randomUUID().toString();
        Map<String, Object> data = Map.of(key, value);
        Instant startTime = Instant.now().truncatedTo(ChronoUnit.MICROS).minusSeconds(5);
        Event event = createEvent(data, startTime);

        Map<String, Object> dataMap = event.toMap();
        Map<String, Object> metadataMap = objectMapper.convertValue(event.getMetadata(), Map.class);
        String input = "{\"" + EventJsonDefines.VERSION + "\":\"" + DataPrepperVersion.getCurrentVersion().toString() + "\", \"" + EventJsonDefines.EVENTS + "\":[";
        String comma = "";
        for (int i = 0; i < 2; i++) {
            input += comma + "{\"data\":" + objectMapper.writeValueAsString(dataMap) + "," + "\"metadata\":" + objectMapper.writeValueAsString(metadataMap) + "}";
            comma = ",";
        }
        input += "]}";
        inputStream = new ByteArrayInputStream(input.getBytes());
        List<Record<Event>> records = new LinkedList<>();
        inputCodec.parse(inputStream, records::add);
        assertThat(records.size(), equalTo(2));
        for (Record record : records) {
            Event e = (Event) record.getData();
            assertThat(e.get(key, String.class), equalTo(value));
            assertThat(e.getMetadata().getTimeReceived(), not(equalTo(startTime)));
            assertThat(e.getMetadata().getTags().size(), equalTo(0));
            assertThat(e.getMetadata().getExternalOriginationTime(), equalTo(null));
        }
    }


    @ParameterizedTest
    @ValueSource(ints = {32, 64})
    public void parse_with_string_value_within_the_raised_limit_returns_event(final int valueSizeInMB) throws Exception {
        // Jackson's default maximum string length is 20 MB; the codec raises it to 64 MB
        // to match JacksonEvent. String values up to that limit must parse successfully.
        inputCodec = createInputCodec();
        final String key = UUID.randomUUID().toString();
        final String largeValue = buildStringOfSize(valueSizeInMB * 1024 * 1024);
        Map<String, Object> data = Map.of(key, largeValue);
        Instant startTime = Instant.now().truncatedTo(ChronoUnit.MICROS);
        Event event = createEvent(data, startTime);

        Map<String, Object> dataMap = event.toMap();
        Map<String, Object> metadataMap = objectMapper.convertValue(event.getMetadata(), Map.class);
        String input = "{\"" + EventJsonDefines.VERSION + "\":\"" + DataPrepperVersion.getCurrentVersion().toString() + "\", \"" + EventJsonDefines.EVENTS + "\":[" +
                "{\"data\":" + objectMapper.writeValueAsString(dataMap) + "," + "\"metadata\":" + objectMapper.writeValueAsString(metadataMap) + "}" +
                "]}";
        inputStream = new ByteArrayInputStream(input.getBytes());
        List<Record<Event>> records = new LinkedList<>();
        inputCodec.parse(inputStream, records::add);
        assertThat(records.size(), equalTo(1));
        Event resultEvent = records.get(0).getData();
        assertThat(resultEvent.get(key, String.class), equalTo(largeValue));
    }

    @Test
    public void parse_with_string_value_exceeding_the_raised_limit_throws() throws Exception {
        // A string value larger than the 64 MB limit must still be rejected.
        inputCodec = createInputCodec();
        final String key = UUID.randomUUID().toString();
        final String tooLargeValue = buildStringOfSize(65 * 1024 * 1024);
        Map<String, Object> data = Map.of(key, tooLargeValue);
        Instant startTime = Instant.now().truncatedTo(ChronoUnit.MICROS);
        Event event = createEvent(data, startTime);

        Map<String, Object> dataMap = event.toMap();
        Map<String, Object> metadataMap = objectMapper.convertValue(event.getMetadata(), Map.class);
        String input = "{\"" + EventJsonDefines.VERSION + "\":\"" + DataPrepperVersion.getCurrentVersion().toString() + "\", \"" + EventJsonDefines.EVENTS + "\":[" +
                "{\"data\":" + objectMapper.writeValueAsString(dataMap) + "," + "\"metadata\":" + objectMapper.writeValueAsString(metadataMap) + "}" +
                "]}";
        inputStream = new ByteArrayInputStream(input.getBytes());
        List<Record<Event>> records = new LinkedList<>();
        assertThrows(StreamConstraintsException.class, () -> inputCodec.parse(inputStream, records::add));
        assertThat(records.size(), equalTo(0));
    }

    private static String buildStringOfSize(final int length) {
        final StringBuilder builder = new StringBuilder(length);
        while (builder.length() < length) {
            builder.append('a');
        }
        return builder.toString();
    }

    private Event createEvent(final Map<String, Object> json, final Instant timeReceived) {
        final JacksonLog.Builder logBuilder = JacksonLog.builder()
                .withData(json)
                .getThis();
        if (timeReceived != null) {
            logBuilder.withTimeReceived(timeReceived);
        }
        final JacksonEvent event = (JacksonEvent) logBuilder.build();

        return event;
    }
}

