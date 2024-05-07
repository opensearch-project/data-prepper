/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.model.changestream.OperationType;
import org.opensearch.dataprepper.model.document.JacksonDocument;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;


/**
 * The record convert transform the source data into a JacksonEvent.
 */
public class RecordConverter {
    private static final Logger LOG = LoggerFactory.getLogger(RecordConverter.class);
    private static final String DEFAULT_ACTION = OpenSearchBulkActions.INDEX.toString();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String collection;
    private final String dataType;

    public RecordConverter(final String collection, final String dataType) {
        this.collection = collection;
        this.dataType = dataType;
    }

    /**
     * Extract the value based on attribute map
     *
     * @param data          A map of attribute name and value
     * @param attributeName Attribute name
     * @return the related attribute value, return null if the attribute name doesn't exist.
     */
    private String getAttributeValue(final Map<String, Object> data, final String attributeName) {
        if (data.containsKey(attributeName)) {
            final Object value = data.get(attributeName);
            return String.valueOf(value);
        }
        return null;
    }

    /**
     * Convert the source data into a JacksonEvent.
     *
     * @param record                  record that will be converted to Event.
     * @param eventCreateTimeEpochMillis Creation timestamp of the event in epoch millis
     * @param eventVersionNumber      Event version number to handle conflicts
     * @param eventName               Event name
     * @return Jackson document event
     */
    public Event convert(final String record,
                        final long eventCreateTimeEpochMillis,
                        final long eventVersionNumber,
                        final OperationType eventName,
                        final String primaryKeyBsonType) {
        final Map<String, Object> data = convertToMap(record);
        final Event event = JacksonDocument.builder()
                .withData(data)
                .build();

        // Only set external origination time for stream events, not export
        if (eventName != null) {
            final Instant externalOriginationTime = Instant.ofEpochMilli(eventCreateTimeEpochMillis);
            event.getEventHandle().setExternalOriginationTime(externalOriginationTime);
            event.getMetadata().setExternalOriginationTime(externalOriginationTime);
        }
        final EventMetadata eventMetadata = event.getMetadata();

        eventMetadata.setAttribute(MetadataKeyAttributes.INGESTION_EVENT_TYPE_ATTRIBUTE, dataType);
        eventMetadata.setAttribute(MetadataKeyAttributes.DOCUMENTDB_EVENT_COLLECTION_METADATA_ATTRIBUTE, collection);
        eventMetadata.setAttribute(MetadataKeyAttributes.DOCUMENTDB_EVENT_TIMESTAMP_METADATA_ATTRIBUTE, eventCreateTimeEpochMillis);
        eventMetadata.setAttribute(MetadataKeyAttributes.DOCUMENTDB_STREAM_EVENT_NAME_METADATA_ATTRIBUTE, eventName);
        eventMetadata.setAttribute(MetadataKeyAttributes.EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE, mapStreamEventNameToBulkAction(eventName));
        eventMetadata.setAttribute(MetadataKeyAttributes.EVENT_VERSION_FROM_TIMESTAMP, eventVersionNumber);

        final String partitionKey = getAttributeValue(data, MetadataKeyAttributes.DOCUMENTDB_PRIMARY_KEY_ATTRIBUTE_NAME);
        eventMetadata.setAttribute(MetadataKeyAttributes.PARTITION_KEY_METADATA_ATTRIBUTE, partitionKey);
        eventMetadata.setAttribute(MetadataKeyAttributes.PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE, partitionKey);
        eventMetadata.setAttribute(MetadataKeyAttributes.DOCUMENTDB_ID_TYPE_METADATA_ATTRIBUTE, primaryKeyBsonType);

        return event;
    }

    /**
     * Convert the source data into a JacksonEvent.
     *
     * @param record                  record that will be converted to Event.
     * @param eventCreationTimeMillis Creation timestamp of the event
     * @param eventVersionNumber      Event version number to handle conflicts
     * @return Jackson document event
     */
    public Event convert(final String record,
                        final long eventCreationTimeMillis,
                        final long eventVersionNumber,
                        final String primaryKeyBsonType) {
        return convert(record, eventCreationTimeMillis, eventVersionNumber, null, primaryKeyBsonType);
    }

    private String mapStreamEventNameToBulkAction(final OperationType streamEventName) {
        if (streamEventName == null) {
            return DEFAULT_ACTION;
        }

        // https://www.mongodb.com/docs/manual/reference/change-events/
        switch (streamEventName) {
            case INSERT:
            case UPDATE:
            case REPLACE:
                return OpenSearchBulkActions.INDEX.toString();
            case DELETE:
                return OpenSearchBulkActions.DELETE.toString();
            default:
                return DEFAULT_ACTION;
        }
    }


    private Map<String, Object> convertToMap(String jsonData) {
        try {
            return MAPPER.readValue(jsonData, Map.class);
        } catch (final JsonProcessingException e) {
            LOG.error("Error converting json data into map.");
            return null;
        }
    }
}
