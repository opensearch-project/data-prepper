/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.event_json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
        input = "{\""+EventJsonDefines.VERSION+"\":\""+DataPrepperVersion.getCurrentVersion().toString()+"\", \""+EventJsonDefines.EVENTS+"\":["+input+"]}";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(input.getBytes());
        inputCodec = createInputCodec();
        Consumer<Record<Event>> consumer = mock(Consumer.class);
        inputCodec.parse(inputStream, consumer);
        verifyNoInteractions(consumer);
    }

    @Test
    public void invalidVersionTest() throws Exception {
        inputCodec = createInputCodec();
        final String key = UUID.randomUUID().toString();
        final String value = UUID.randomUUID().toString();
        Map<String, Object> data = Map.of(key, value);
        Instant startTime = Instant.now();
        Event event = createEvent(data, startTime);

        Map<String, Object> dataMap = event.toMap();
        Map<String, Object> metadataMap = objectMapper.convertValue(event.getMetadata(), Map.class);
        String input = "{\""+EventJsonDefines.VERSION+"\":\"2.0\", \""+EventJsonDefines.EVENTS+"\":[";
        String comma = "";
        for (int i = 0; i < 2; i++) {
            input += comma+"{\"data\":"+objectMapper.writeValueAsString(dataMap)+","+"\"metadata\":"+objectMapper.writeValueAsString(metadataMap)+"}";
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
        Instant startTime = Instant.now();
        Event event = createEvent(data, startTime);

        Map<String, Object> dataMap = event.toMap();
        Map<String, Object> metadataMap = objectMapper.convertValue(event.getMetadata(), Map.class);
        String input = "{\""+EventJsonDefines.VERSION+"\":\""+DataPrepperVersion.getCurrentVersion().toString()+"\", \""+EventJsonDefines.EVENTS+"\":[";
        String comma = "";
        for (int i = 0; i < 2; i++) {
            input += comma+"{\"data\":"+objectMapper.writeValueAsString(dataMap)+","+"\"metadata\":"+objectMapper.writeValueAsString(metadataMap)+"}";
            comma = ",";
        }
        input += "]}";
        inputStream = new ByteArrayInputStream(input.getBytes());
        List<Record<Event>> records = new LinkedList<>();
        inputCodec.parse(inputStream, records::add);
        assertThat(records.size(), equalTo(2));
        for(Record record : records) {
            Event e = (Event)record.getData();
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
        Instant startTime = Instant.now().minusSeconds(5);
        Event event = createEvent(data, startTime);

        Map<String, Object> dataMap = event.toMap();
        Map<String, Object> metadataMap = objectMapper.convertValue(event.getMetadata(), Map.class);
        String input = "{\""+EventJsonDefines.VERSION+"\":\""+DataPrepperVersion.getCurrentVersion().toString()+"\", \""+EventJsonDefines.EVENTS+"\":[";
        String comma = "";
        for (int i = 0; i < 2; i++) {
            input += comma+"{\"data\":"+objectMapper.writeValueAsString(dataMap)+","+"\"metadata\":"+objectMapper.writeValueAsString(metadataMap)+"}";
            comma = ",";
        }
        input += "]}";
        inputStream = new ByteArrayInputStream(input.getBytes());
        List<Record<Event>> records = new LinkedList<>();
        inputCodec.parse(inputStream, records::add);
        assertThat(records.size(), equalTo(2));
        for(Record record : records) {
            Event e = (Event)record.getData();
            assertThat(e.get(key, String.class), equalTo(value));
            assertThat(e.getMetadata().getTimeReceived(), not(equalTo(startTime)));
            assertThat(e.getMetadata().getTags().size(), equalTo(0));
            assertThat(e.getMetadata().getExternalOriginationTime(), equalTo(null));
        }
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

