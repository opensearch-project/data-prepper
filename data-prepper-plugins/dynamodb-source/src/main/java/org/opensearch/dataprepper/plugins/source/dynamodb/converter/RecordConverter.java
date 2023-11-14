/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.converter;

import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableInfo;

import java.time.Instant;
import java.util.Map;

import static org.opensearch.dataprepper.plugins.source.dynamodb.converter.MetadataKeyAttributes.DDB_STREAM_EVENT_NAME_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.dynamodb.converter.MetadataKeyAttributes.EVENT_VERSION_FROM_TIMESTAMP;
import static org.opensearch.dataprepper.plugins.source.dynamodb.converter.MetadataKeyAttributes.EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.dynamodb.converter.MetadataKeyAttributes.EVENT_TABLE_NAME_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.dynamodb.converter.MetadataKeyAttributes.EVENT_TIMESTAMP_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.dynamodb.converter.MetadataKeyAttributes.PARTITION_KEY_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.dynamodb.converter.MetadataKeyAttributes.PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.dynamodb.converter.MetadataKeyAttributes.SORT_KEY_METADATA_ATTRIBUTE;

/**
 * Base Record Processor definition.
 * The record processor is to transform the source data into a JacksonEvent,
 * and then write to buffer.
 */
public abstract class RecordConverter {

    private static final String DEFAULT_ACTION = OpenSearchBulkActions.INDEX.toString();


    private final BufferAccumulator<Record<Event>> bufferAccumulator;

    private final TableInfo tableInfo;

    public RecordConverter(final BufferAccumulator<Record<Event>> bufferAccumulator, TableInfo tableInfo) {
        this.bufferAccumulator = bufferAccumulator;
        this.tableInfo = tableInfo;
    }

    abstract String getEventType();

    /**
     * Extract the value based on attribute map
     *
     * @param data          A map of attribute name and value
     * @param attributeName Attribute name
     * @return the related attribute value, return null if the attribute name doesn't exist.
     */
    private String getAttributeValue(final Map<String, Object> data, String attributeName) {
        if (data.containsKey(attributeName)) {
            return String.valueOf(data.get(attributeName));
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
     * @param keys                    A map to hold the keys (partition key and sort key)
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
        Event event = JacksonEvent.builder()
                .withEventType(getEventType())
                .withData(data)
                .build();

        // Only set external origination time for stream events, not export
        if (eventName != null) {
            final Instant externalOriginationTime = Instant.ofEpochMilli(eventCreationTimeMillis);
            event.getEventHandle().setExternalOriginationTime(externalOriginationTime);
            event.getMetadata().setExternalOriginationTime(externalOriginationTime);
        }
        EventMetadata eventMetadata = event.getMetadata();

        eventMetadata.setAttribute(EVENT_TABLE_NAME_METADATA_ATTRIBUTE, tableInfo.getTableName());
        eventMetadata.setAttribute(EVENT_TIMESTAMP_METADATA_ATTRIBUTE, eventCreationTimeMillis);
        eventMetadata.setAttribute(DDB_STREAM_EVENT_NAME_METADATA_ATTRIBUTE, eventName);
        eventMetadata.setAttribute(EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE, mapStreamEventNameToBulkAction(eventName));
        eventMetadata.setAttribute(EVENT_VERSION_FROM_TIMESTAMP, eventVersionNumber);

        String partitionKey = getAttributeValue(keys, tableInfo.getMetadata().getPartitionKeyAttributeName());
        eventMetadata.setAttribute(PARTITION_KEY_METADATA_ATTRIBUTE, partitionKey);

        String sortKey = getAttributeValue(keys, tableInfo.getMetadata().getSortKeyAttributeName());
        if (sortKey != null) {
            eventMetadata.setAttribute(SORT_KEY_METADATA_ATTRIBUTE, sortKey);
            eventMetadata.setAttribute(PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE, partitionKey + "|" + sortKey);
        } else {
            eventMetadata.setAttribute(PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE, partitionKey);
        }
        if (acknowledgementSet != null) {
            acknowledgementSet.add(event);
        }
        bufferAccumulator.add(new Record<>(event));
    }

    public void addToBuffer(final AcknowledgementSet acknowledgementSet, Map<String, Object> data) throws Exception {
        // Export data doesn't have an event timestamp
        // We consider this to be time of 0, meaning stream records will always be considered as newer
        // than export records
        addToBuffer(acknowledgementSet, data, data, System.currentTimeMillis(), 0L, null);
    }

    private String mapStreamEventNameToBulkAction(final String streamEventName) {
        if (streamEventName == null) {
            return DEFAULT_ACTION;
        }

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

}
