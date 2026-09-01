/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.opensearchapi;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.model.codec.ByteDecoder;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventType;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.opensearchapi.model.BulkAPIEventMetadataKeyAttributes;
import org.opensearch.dataprepper.plugins.source.opensearchapi.model.BulkAPIRequestParams;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Consumer;

public class OpenSearchBulkByteDecoder implements ByteDecoder {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    @Override
    public void parse(InputStream inputStream, Instant timeReceived, Consumer<Record<Event>> eventConsumer) throws IOException {
        parse(inputStream, timeReceived, null, eventConsumer);
    }

    public void parse(InputStream inputStream, Instant timeReceived, BulkAPIRequestParams requestParams, Consumer<Record<Event>> eventConsumer) throws IOException {
        final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.isBlank()) continue;

            Map<String, Object> actionLine = OBJECT_MAPPER.readValue(line, MAP_TYPE);
            String action = extractAction(actionLine);
            if (action == null) continue;

            @SuppressWarnings("unchecked")
            Map<String, Object> metadata = (Map<String, Object>) actionLine.get(action);
            boolean isDelete = OpenSearchBulkActions.DELETE.toString().equals(action);

            Map<String, Object> documentData = null;
            if (!isDelete) {
                String docLine = reader.readLine();
                if (docLine == null || docLine.isBlank()) continue;
                documentData = OBJECT_MAPPER.readValue(docLine, MAP_TYPE);
            }

            JacksonEvent.Builder builder = JacksonEvent.builder()
                    .withEventType(EventType.DOCUMENT.toString());
            if (documentData != null) {
                builder.withData(documentData);
            }
            if (timeReceived != null) {
                builder.withTimeReceived(timeReceived);
            }
            JacksonEvent event = builder.build();

            event.getMetadata().setAttribute(
                    BulkAPIEventMetadataKeyAttributes.BULK_API_EVENT_METADATA_ATTRIBUTE_ACTION, action);

            // Set index: action line takes priority, then URL-level fallback
            String index = getMetadataValue(metadata, "_index");
            if (isBlank(index) && requestParams != null && !isBlank(requestParams.getIndex())) {
                index = requestParams.getIndex();
            }
            if (!isBlank(index)) {
                event.getMetadata().setAttribute(BulkAPIEventMetadataKeyAttributes.BULK_API_EVENT_METADATA_ATTRIBUTE_INDEX, index);
            }

            // Set id
            setIfPresent(event, metadata, "_id", BulkAPIEventMetadataKeyAttributes.BULK_API_EVENT_METADATA_ATTRIBUTE_ID);

            // Set routing: action line takes priority, then URL-level fallback
            String routing = getMetadataValue(metadata, "routing");
            if (isBlank(routing) && requestParams != null && !isBlank(requestParams.getRouting())) {
                routing = requestParams.getRouting();
            }
            if (!isBlank(routing)) {
                event.getMetadata().setAttribute(BulkAPIEventMetadataKeyAttributes.BULK_API_EVENT_METADATA_ATTRIBUTE_ROUTING, routing);
            }

            // Set pipeline: action line takes priority, then URL-level fallback
            String pipeline = getMetadataValue(metadata, "pipeline");
            if (isBlank(pipeline) && requestParams != null && !isBlank(requestParams.getPipeline())) {
                pipeline = requestParams.getPipeline();
            }
            if (!isBlank(pipeline)) {
                event.getMetadata().setAttribute(BulkAPIEventMetadataKeyAttributes.BULK_API_EVENT_METADATA_ATTRIBUTE_PIPELINE, pipeline);
            }

            eventConsumer.accept(new Record<>(event));
        }
    }

    private String extractAction(Map<String, Object> actionLine) {
        return Arrays.stream(OpenSearchBulkActions.values())
                .map(OpenSearchBulkActions::toString)
                .filter(actionLine::containsKey)
                .findFirst()
                .orElse(null);
    }

    private String getMetadataValue(Map<String, Object> metadata, String key) {
        if (metadata == null) return null;
        Object value = metadata.get(key);
        return value != null ? value.toString() : null;
    }

    private void setIfPresent(JacksonEvent event, Map<String, Object> metadata, String key, String attribute) {
        if (metadata == null) return;
        Object value = metadata.get(key);
        if (value != null) {
            event.getMetadata().setAttribute(attribute, value.toString());
        }
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
