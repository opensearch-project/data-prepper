/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.event_json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;


import java.io.ByteArrayOutputStream;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public class EventJsonOutputCodecTest {
    private static final Integer BYTEBUFFER_SIZE = 1024;
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    @Mock
    private EventJsonOutputCodecConfig eventJsonOutputCodecConfig;

    private EventJsonOutputCodec outputCodec;
    private ByteArrayOutputStream outputStream;

    @BeforeEach
    public void setup() {
        outputStream = new ByteArrayOutputStream(BYTEBUFFER_SIZE);
    }

    public EventJsonOutputCodec createOutputCodec() {
        return new EventJsonOutputCodec(eventJsonOutputCodecConfig);
    }

    @Test
    public void basicTest() throws Exception {
        final String key = UUID.randomUUID().toString();
        final String value = UUID.randomUUID().toString();
        Map<String, Object> data = Map.of(key, value);

        Instant startTime = Instant.now();
        Event event = createEvent(data, startTime);
        outputCodec = createOutputCodec();
        outputCodec.start(outputStream, null, null);
        outputCodec.writeEvent(event, outputStream);
        outputCodec.writeEvent(event, outputStream);
        outputCodec.complete(outputStream);
        Map<String, Object> dataMap = event.toMap();
        Map<String, Object> metadataMap = objectMapper.convertValue(event.getMetadata(), Map.class);
        String expectedOutput = "[";
        String comma = "";
        for (int i = 0; i < 2; i++) {
            expectedOutput += comma+"{\"data\":"+objectMapper.writeValueAsString(dataMap)+","+"\"metadata\":"+objectMapper.writeValueAsString(metadataMap)+"}";
            comma = ",";
        }
        expectedOutput += "]";
        String output = outputStream.toString();
        assertThat(output, equalTo(expectedOutput));

    }

    private Event createEvent(final Map<String, Object> json, final Instant timeReceived) {
        final JacksonLog.Builder logBuilder = JacksonLog.builder()
                .withData(json)
                .getThis();
        if (timeReceived != null) {
            logBuilder.withTimeReceived(timeReceived);
        }
        final JacksonEvent event = (JacksonEvent)logBuilder.build();

        return event;
    }
}
