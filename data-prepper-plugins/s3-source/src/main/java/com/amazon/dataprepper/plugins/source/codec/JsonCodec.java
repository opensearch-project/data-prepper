/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.codec;

import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.JacksonEvent;
import com.amazon.dataprepper.model.log.JacksonLog;
import com.amazon.dataprepper.model.record.Record;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * An implementation of {@link Codec} which parses JSON objects for arrays.
 */
public class JsonCodec implements Codec {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final JsonFactory jsonFactory = new JsonFactory();

    @Override
    public void parse(final InputStream inputStream, final Consumer<Record<Event>> eventConsumer) throws IOException {

        Objects.requireNonNull(inputStream);
        Objects.requireNonNull(eventConsumer);

        final JsonParser jsonParser = jsonFactory.createParser(inputStream);

        while (!jsonParser.isClosed() && jsonParser.nextToken() != JsonToken.END_OBJECT) {
            if (jsonParser.getCurrentToken() == JsonToken.START_ARRAY) {
                parseRecordsArray(jsonParser, eventConsumer);
            }
        }
    }

    private void parseRecordsArray(final JsonParser jsonParser, final Consumer<Record<Event>> eventConsumer) throws IOException {
        while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
            final Map<String, Object> innerJson = objectMapper.readValue(jsonParser, Map.class);

            final Record<Event> record = createRecord(innerJson);
            eventConsumer.accept(record);
        }
    }

    private Record<Event> createRecord(final Map<String, Object> json) {
        final JacksonEvent event = JacksonLog.builder()
                .withData(json)
                .build();

        return new Record<>(event);
    }
}
