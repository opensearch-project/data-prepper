/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.mongo.configuration.CollectionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;


/**
 * Base Record Processor definition.
 * The record processor is to transform the source data into a JacksonEvent,
 * and then write to buffer.
 */
public abstract class RecordConverter {
    private static final Logger LOG = LoggerFactory.getLogger(RecordConverter.class);

    private static final String DEFAULT_ACTION = OpenSearchBulkActions.INDEX.toString();

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final BufferAccumulator<Record<Event>> bufferAccumulator;

    final CollectionConfig collectionConfig;

    public RecordConverter(final BufferAccumulator<Record<Event>> bufferAccumulator, final CollectionConfig collectionConfig) {
        this.bufferAccumulator = bufferAccumulator;
        this.collectionConfig = collectionConfig;
    }

    abstract String getEventType();

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

    void flushBuffer() throws Exception {
        bufferAccumulator.flush();
    }

    /**
     * Add event record to buffer
     *
     * @param data                    A map to hold event data, note that it may be empty.
     * @param keys                    A map to hold the keys (partition key)
     * @param eventCreationTimeMillis Creation timestamp of the event
     * @param eventName               Event name
     * @throws Exception Exception if failed to write to buffer.
     */
    public void addToBuffer(final AcknowledgementSet acknowledgementSet,
                            final Map<String, Object> data,
                            final Map<String, Object> keys,
                            final long eventCreationTimeMillis,
                            final long eventVersionNumber,
                            final String eventName) throws Exception {
        final Event event = JacksonEvent.builder()
                .withEventType(getEventType())
                .withData(data)
                .build();

        // Only set external origination time for stream events, not export
        if (eventName != null) {
            final Instant externalOriginationTime = Instant.ofEpochMilli(eventCreationTimeMillis);
            event.getEventHandle().setExternalOriginationTime(externalOriginationTime);
            event.getMetadata().setExternalOriginationTime(externalOriginationTime);
        }
        final EventMetadata eventMetadata = event.getMetadata();

        eventMetadata.setAttribute(MetadataKeyAttributes.MONGODB_EVENT_COLLECTION_METADATA_ATTRIBUTE, collectionConfig.getCollection());
        eventMetadata.setAttribute(MetadataKeyAttributes.MONGODB_EVENT_TIMESTAMP_METADATA_ATTRIBUTE, eventCreationTimeMillis);
        eventMetadata.setAttribute(MetadataKeyAttributes.MONGODB_STREAM_EVENT_NAME_METADATA_ATTRIBUTE, eventName);
        eventMetadata.setAttribute(MetadataKeyAttributes.EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE, mapStreamEventNameToBulkAction(eventName));
        eventMetadata.setAttribute(MetadataKeyAttributes.EVENT_VERSION_FROM_TIMESTAMP, eventVersionNumber);

        final String partitionKey = getAttributeValue(keys, MetadataKeyAttributes.MONGODB_PRIMARY_KEY_ATTRIBUTE_NAME);
        eventMetadata.setAttribute(MetadataKeyAttributes.PARTITION_KEY_METADATA_ATTRIBUTE, partitionKey);
        eventMetadata.setAttribute(MetadataKeyAttributes.PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE, partitionKey);

        if (acknowledgementSet != null) {
            acknowledgementSet.add(event);
        }
        bufferAccumulator.add(new Record<>(event));
    }

    public void addToBuffer(final AcknowledgementSet acknowledgementSet,
                            final Map<String, Object> data,
                            final long timestamp,
                            final long eventVersionNumber) throws Exception {
        addToBuffer(acknowledgementSet, data, data, timestamp, eventVersionNumber, null);
    }

    private String mapStreamEventNameToBulkAction(final String streamEventName) {
        if (streamEventName == null) {
            return DEFAULT_ACTION;
        }

        // https://www.mongodb.com/docs/manual/reference/change-events/
        switch (streamEventName) {
            case "INSERT":
            case "MODIFY":
                return OpenSearchBulkActions.INDEX.toString();
            case "REMOVE":
                return OpenSearchBulkActions.DELETE.toString();
            default:
                return DEFAULT_ACTION;
        }
    }

    Map<String, Object> convertToMap(String jsonData) {
        try {
            return MAPPER.readValue(jsonData, Map.class);
        } catch (final JsonProcessingException e) {
            LOG.error("Error converting json data into map.");
            return null;
        }
    }

}
