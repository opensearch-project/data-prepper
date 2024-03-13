/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.codec;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

public class JsonDecoder implements ByteDecoder {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final JsonFactory jsonFactory = new JsonFactory();

    public void parse(InputStream inputStream, Instant timeReceived, Consumer<Record<Event>> eventConsumer) throws IOException {
        Objects.requireNonNull(inputStream);
        Objects.requireNonNull(eventConsumer);

        final JsonParser jsonParser = jsonFactory.createParser(inputStream);

        while (!jsonParser.isClosed() && jsonParser.nextToken() != JsonToken.END_OBJECT) {
            if (jsonParser.getCurrentToken() == JsonToken.START_ARRAY) {
                parseRecordsArray(jsonParser, timeReceived, eventConsumer);
            }
        }
    }

    private void parseRecordsArray(final JsonParser jsonParser, final Instant timeReceived, final Consumer<Record<Event>> eventConsumer) throws IOException {
        while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
            final Map<String, Object> innerJson = objectMapper.readValue(jsonParser, Map.class);

            final Record<Event> record = createRecord(innerJson, timeReceived);
            eventConsumer.accept(record);
        }
    }

    private Record<Event> createRecord(final Map<String, Object> json, final Instant timeReceived) {
        final JacksonLog.Builder logBuilder = JacksonLog.builder()
                .withData(json)
                .getThis();
        if (timeReceived != null) {
            logBuilder.withTimeReceived(timeReceived);
        }
        final JacksonEvent event = (JacksonEvent)logBuilder.build();

        return new Record<>(event);
    }

}
