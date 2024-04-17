/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec.eventjson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.function.Consumer;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * An implementation of {@link InputCodec} which parses JSON Objects for arrays.
 */
@DataPrepperPlugin(name = "event_json", pluginType = InputCodec.class)
public class EventJsonInputCodec implements InputCodec {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final JsonFactory jsonFactory = new JsonFactory();

    public void parse(InputStream inputStream, Consumer<Record<Event>> eventConsumer) throws IOException {
        Objects.requireNonNull(inputStream);
        Objects.requireNonNull(eventConsumer);

        final JsonParser jsonParser = jsonFactory.createParser(inputStream);
        while (!jsonParser.isClosed() && jsonParser.nextToken() != JsonToken.END_OBJECT) {
            if (jsonParser.getCurrentToken() == JsonToken.START_OBJECT) {
                final Map<String, Object> innerJson = objectMapper.readValue(jsonParser, Map.class);

                Map<String, Object> metadata = (Map<String, Object>)innerJson.get(EventJsonDefines.METADATA);
                Map<String, Object> data = (Map<String, Object>)innerJson.get(EventJsonDefines.DATA);
                final JacksonLog.Builder logBuilder = JacksonLog.builder()
                    .withData(data)
                    .withEventMetadataAttributes((Map<String, Object>)metadata.get(EventJsonDefines.ATTRIBUTES))
                    .withTimeReceived(Instant.parse((String)metadata.get(EventJsonDefines.TIME_RECEIVED)))
                    .getThis();
                final JacksonEvent event = (JacksonEvent)logBuilder.build();
                final Record<Event> record =  new Record<>(event);
                final String externalOriginationTime = (String)metadata.get(EventJsonDefines.EXTERNAL_ORIGINATION_TIME);
                final List<String> tags = (List<String>)metadata.get(EventJsonDefines.TAGS);
                if (tags.size() > 0) {
                    event.getMetadata().addTags(tags);
                }
                if (externalOriginationTime != null) {
                    event.getMetadata().setExternalOriginationTime(Instant.parse(externalOriginationTime));
                }
                eventConsumer.accept(record);
            }
        }
    }

}

