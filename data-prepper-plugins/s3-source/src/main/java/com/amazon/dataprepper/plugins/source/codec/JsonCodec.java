/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.codec;

import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.JacksonEvent;
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
    public void parse(InputStream inputStream, Consumer<Record<Event>> eventConsumer) throws IOException {

        Objects.requireNonNull(inputStream);
        Objects.requireNonNull(eventConsumer);

        JsonParser jsonParser = jsonFactory.createParser(inputStream);

        boolean inArrayToParse = false;
        while (!jsonParser.isClosed() && jsonParser.nextToken() != JsonToken.END_OBJECT) {

            if (jsonParser.getCurrentToken() == JsonToken.END_ARRAY) {
                inArrayToParse = false;
            }

            if (inArrayToParse) {
                final Map innerJson = objectMapper.readValue(jsonParser, Map.class);

                final Record<Event> record = createRecord(innerJson);
                eventConsumer.accept(record);
            }

            if (jsonParser.getCurrentToken() == JsonToken.START_ARRAY) {
                inArrayToParse = true;
            }
        }
    }

    private Record<Event> createRecord(Map json) {
        final JacksonEvent event = JacksonEvent.builder()
                .withEventType("event")
                .withData(json)
                .build();

        return new Record<>(event);
    }
}
