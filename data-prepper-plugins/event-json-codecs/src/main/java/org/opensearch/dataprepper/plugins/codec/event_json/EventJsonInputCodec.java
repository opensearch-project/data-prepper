/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec.event_json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.DefaultEventMetadata;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.function.Consumer;
import java.util.Map;
import java.util.Objects;

/**
 * An implementation of {@link InputCodec} which parses JSON Objects for arrays.
 */
@DataPrepperPlugin(name = "event_json", pluginType = InputCodec.class, pluginConfigurationType = EventJsonInputCodecConfig.class)
public class EventJsonInputCodec implements InputCodec {
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private final JsonFactory jsonFactory = new JsonFactory();
    private final Boolean overrideTimeReceived;

    @DataPrepperPluginConstructor
    public EventJsonInputCodec(final EventJsonInputCodecConfig config) {
        this.overrideTimeReceived = config.getOverrideTimeReceived();
    }

    public void parse(InputStream inputStream, Consumer<Record<Event>> eventConsumer) throws IOException {
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
            if (record != null) {
                eventConsumer.accept(record);
            }
        }
    }

    private Record<Event> createRecord(final Map<String, Object> innerJson) {
        Map<String, Object> metadata = (Map<String, Object>)innerJson.get(EventJsonDefines.METADATA);
        EventMetadata eventMetadata = objectMapper.convertValue(metadata, DefaultEventMetadata.class);
        Map<String, Object> data = (Map<String, Object>)innerJson.get(EventJsonDefines.DATA);
        if (data == null) {
            return null;
        }
        if (overrideTimeReceived) {
            eventMetadata = new DefaultEventMetadata.Builder()
                .withAttributes(eventMetadata.getAttributes())
                .withTimeReceived(Instant.now())
                .withTags(eventMetadata.getTags())
                .withExternalOriginationTime(eventMetadata.getExternalOriginationTime())
                .build();
        }
        final JacksonLog.Builder logBuilder = JacksonLog.builder()
            .withData(data)
            .withEventMetadata(eventMetadata)
            .getThis();
        final JacksonEvent event = (JacksonEvent)logBuilder.build();
        final Record<Event> record =  new Record<>(event);
        return record;
    }
}
