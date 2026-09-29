/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.dataprepper.plugins.codec.event_json;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;

import org.mockito.Mock;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;


import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class EventJsonInputOutputCodecTest {
    private static final Integer BYTEBUFFER_SIZE = 1024;

    private ByteArrayOutputStream outputStream;

    @Mock
    private EventJsonOutputCodecConfig eventJsonOutputCodecConfig;

    @Mock
    private EventJsonInputCodecConfig eventJsonInputCodecConfig;

    private EventJsonOutputCodec outputCodec;
    private EventJsonInputCodec inputCodec;

    @BeforeEach
    public void setup() {
        outputStream = new ByteArrayOutputStream(BYTEBUFFER_SIZE);
        eventJsonInputCodecConfig = mock(EventJsonInputCodecConfig.class);
    }

    public EventJsonOutputCodec createOutputCodec() {
        return new EventJsonOutputCodec(eventJsonOutputCodecConfig);
    }

    public EventJsonInputCodec createInputCodec() {
        when(eventJsonInputCodecConfig.getOverrideTimeReceived()).thenReturn(true);
        return new EventJsonInputCodec(eventJsonInputCodecConfig);
    }

    @Test
    public void basicTest() throws Exception {
        final String key = UUID.randomUUID().toString();
        final String value = UUID.randomUUID().toString();
        Map<String, Object> data = Map.of(key, value);

        Instant startTime = Instant.now().truncatedTo(ChronoUnit.MICROS);
        Event event = createEvent(data, startTime);
        outputCodec = createOutputCodec();
        inputCodec = createInputCodec();
        outputCodec.start(outputStream, null, null);
        outputCodec.writeEvent(event, outputStream);
        outputCodec.complete(outputStream);
        List<Record<Event>> records = new LinkedList<>();
        inputCodec.parse(new ByteArrayInputStream(outputStream.toByteArray()), records::add);

        assertThat(records.size(), equalTo(1));
        for (Record record : records) {
            Event e = (Event) record.getData();
            assertThat(e.get(key, String.class), equalTo(value));
            assertThat(e.getMetadata().getTimeReceived(), equalTo(startTime));
            assertThat(e.getMetadata().getTags().size(), equalTo(0));
            assertThat(e.getMetadata().getExternalOriginationTime(), equalTo(null));
        }
    }

    @Test
    public void multipleEventsTest() throws Exception {
        final String key = UUID.randomUUID().toString();
        final String value = UUID.randomUUID().toString();
        Map<String, Object> data = Map.of(key, value);

        Instant startTime = Instant.now().truncatedTo(ChronoUnit.MICROS);
        Event event = createEvent(data, startTime);
        outputCodec = createOutputCodec();
        inputCodec = createInputCodec();
        outputCodec.start(outputStream, null, null);
        outputCodec.writeEvent(event, outputStream);
        outputCodec.writeEvent(event, outputStream);
        outputCodec.writeEvent(event, outputStream);
        outputCodec.complete(outputStream);
        List<Record<Event>> records = new LinkedList<>();
        inputCodec.parse(new ByteArrayInputStream(outputStream.toByteArray()), records::add);

        assertThat(records.size(), equalTo(3));
        for (Record record : records) {
            Event e = (Event) record.getData();
            assertThat(e.get(key, String.class), equalTo(value));
            assertThat(e.getMetadata().getTimeReceived(), equalTo(startTime));
            assertThat(e.getMetadata().getTags().size(), equalTo(0));
            assertThat(e.getMetadata().getExternalOriginationTime(), equalTo(null));
        }
    }

    @Test
    public void extendedTest() throws Exception {
        final String key = UUID.randomUUID().toString();
        final String value = UUID.randomUUID().toString();
        final String attrKey = UUID.randomUUID().toString();
        final String attrValue = UUID.randomUUID().toString();
        Map<String, Object> data = Map.of(key, value);

        Set<String> tags = Set.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        List<String> tagsList = tags.stream().collect(Collectors.toList());
        Instant startTime = Instant.now().truncatedTo(ChronoUnit.MICROS);
        Event event = createEvent(data, startTime);
        Instant origTime = startTime.minusSeconds(5);
        event.getMetadata().setExternalOriginationTime(origTime);
        event.getMetadata().addTags(tagsList);
        event.getMetadata().setAttribute(attrKey, attrValue);
        outputCodec = createOutputCodec();
        inputCodec = createInputCodec();
        outputCodec.start(outputStream, null, null);
        outputCodec.writeEvent(event, outputStream);
        outputCodec.complete(outputStream);
        assertThat(outputCodec.getExtension(), equalTo(EventJsonOutputCodec.EVENT_JSON));
        List<Record<Event>> records = new LinkedList<>();
        inputCodec.parse(new ByteArrayInputStream(outputStream.toByteArray()), records::add);

        assertThat(records.size(), equalTo(1));
        for (Record record : records) {
            Event e = (Event) record.getData();
            assertThat(e.get(key, String.class), equalTo(value));
            assertThat(e.getMetadata().getTimeReceived(), equalTo(startTime));
            assertThat(e.getMetadata().getTags(), equalTo(tags));
            assertThat(e.getMetadata().getAttributes(), equalTo(Map.of(attrKey, attrValue)));
            assertThat(e.getMetadata().getExternalOriginationTime(), equalTo(origTime));
            assertThat(e.getEventHandle().getExternalOriginationTime(), equalTo(origTime));
        }
    }


    @Test
    public void roundTrip_with_string_value_larger_than_jackson_default_limit_succeeds() throws Exception {
        // Jackson's default maximum string length is 20 MB. An event holding a larger string
        // value (e.g. from a large database JSONB column) is written by the output codec
        // without any constraint, so the input codec must be able to read it back.
        final String key = UUID.randomUUID().toString();
        final int largeValueLength = 30 * 1024 * 1024;
        final StringBuilder valueBuilder = new StringBuilder(largeValueLength);
        while (valueBuilder.length() < largeValueLength) {
            valueBuilder.append('a');
        }
        final String largeValue = valueBuilder.toString();
        Map<String, Object> data = Map.of(key, largeValue);

        Instant startTime = Instant.now().truncatedTo(ChronoUnit.MICROS);
        Event event = createEvent(data, startTime);
        outputCodec = createOutputCodec();
        inputCodec = createInputCodec();
        outputCodec.start(outputStream, null, null);
        outputCodec.writeEvent(event, outputStream);
        outputCodec.complete(outputStream);
        List<Record<Event>> records = new LinkedList<>();
        inputCodec.parse(new ByteArrayInputStream(outputStream.toByteArray()), records::add);

        assertThat(records.size(), equalTo(1));
        Event resultEvent = records.get(0).getData();
        assertThat(resultEvent.get(key, String.class), equalTo(largeValue));
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

