/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec.eventjson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.DefaultEventMetadata;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Consumer;
import java.util.Map;
import java.util.Objects;

/**
 * An implementation of {@link InputCodec} which parses JSON Objects for arrays.
 */
@DataPrepperPlugin(name = "event_json", pluginType = InputCodec.class)
public class EventJsonInputCodec implements InputCodec {
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private final JsonFactory jsonFactory = new JsonFactory();

    public void parse(InputStream inputStream, Consumer<Record<Event>> eventConsumer) throws IOException {
        Objects.requireNonNull(inputStream);
        Objects.requireNonNull(eventConsumer);

        final JsonParser jsonParser = jsonFactory.createParser(inputStream);
        while (!jsonParser.isClosed() && jsonParser.nextToken() != JsonToken.END_OBJECT) {
            if (jsonParser.getCurrentToken() == JsonToken.START_OBJECT) {
                final Map<String, Object> innerJson = objectMapper.readValue(jsonParser, Map.class);

                Map<String, Object> metadata = (Map<String, Object>)innerJson.get(EventJsonDefines.METADATA);
                EventMetadata eventMetadata = objectMapper.convertValue(metadata, DefaultEventMetadata.class);
                Map<String, Object> data = (Map<String, Object>)innerJson.get(EventJsonDefines.DATA);
                final JacksonLog.Builder logBuilder = JacksonLog.builder()
                    .withData(data)
                    .withEventMetadata(eventMetadata)
                    .getThis();
                final JacksonEvent event = (JacksonEvent)logBuilder.build();
                final Record<Event> record =  new Record<>(event);
                eventConsumer.accept(record);
            }
        }
    }

}

