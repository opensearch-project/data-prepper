/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.codec;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

public class JsonDecoder implements ByteDecoder {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final JsonFactory jsonFactory = new JsonFactory();
    private String keyName;
    private Collection<String> includeKeys;
    private Collection<String> includeKeysMetadata;

    public JsonDecoder(String keyName, Collection<String> includeKeys, Collection<String> includeKeysMetadata, Integer maxEventLength) {
        this.keyName = keyName;
        this.includeKeys = includeKeys;
        this.includeKeysMetadata = includeKeysMetadata;
        if (maxEventLength != null) {
        jsonFactory.setStreamReadConstraints(StreamReadConstraints.builder()
                .maxStringLength(maxEventLength)
                .build());
        }
    }

    public JsonDecoder() {
        this.keyName = null;
        this.includeKeys = null;
        this.includeKeysMetadata = null;
    }

    public void parse(InputStream inputStream, Instant timeReceived, Consumer<Record<Event>> eventConsumer) throws IOException {
        Objects.requireNonNull(inputStream);
        Objects.requireNonNull(eventConsumer);

        final JsonParser jsonParser = jsonFactory.createParser(inputStream);

        Map<String, Object> includeKeysMap = new HashMap<>();
        Map<String, Object> includeMetadataKeysMap = new HashMap<>();
        while (!jsonParser.isClosed() && jsonParser.nextToken() != JsonToken.END_OBJECT) {
            final String nodeName = jsonParser.currentName();

            if (includeKeys != null && includeKeys.contains(nodeName) ||
                    (includeKeysMetadata != null && includeKeysMetadata.contains(nodeName))) {
                jsonParser.nextToken();
                if (includeKeys != null && includeKeys.contains(nodeName)) {
                    includeKeysMap.put(nodeName, jsonParser.getValueAsString());
                }
                if (includeKeysMetadata != null && includeKeysMetadata.contains(nodeName)) {
                    includeMetadataKeysMap.put(nodeName, jsonParser.getValueAsString());
                }
                continue;
            }

            if (jsonParser.getCurrentToken() == JsonToken.START_ARRAY) {
                if (keyName != null && !nodeName.equals(keyName)) {
                    continue;
                }
                parseRecordsArray(jsonParser, timeReceived, eventConsumer, includeKeysMap, includeMetadataKeysMap);
            }
        }
    }

    private void parseRecordsArray(final JsonParser jsonParser,
                                   final Instant timeReceived,
                                   final Consumer<Record<Event>> eventConsumer,
                                   final Map<String, Object> includeKeysMap,
                                   final Map<String, Object> includeMetadataKeysMap
    ) throws IOException {
        while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
            final Map<String, Object> innerJson = objectMapper.readValue(jsonParser, Map.class);

            final Record<Event> record = createRecord(innerJson, timeReceived);
            for (final Map.Entry<String, Object> entry : includeKeysMap.entrySet()) {
                record.getData().put(entry.getKey(), entry.getValue());
            }

            for (final Map.Entry<String, Object> entry : includeMetadataKeysMap.entrySet()) {
                record.getData().getMetadata().setAttribute(entry.getKey(), entry.getValue());
            }

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
