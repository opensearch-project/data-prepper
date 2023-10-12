/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.converter;

import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableInfo;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.opensearch.dataprepper.plugins.source.dynamodb.converter.MetadataKeyAttributes.PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.dynamodb.converter.MetadataKeyAttributes.EVENT_TABLE_NAME_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.dynamodb.converter.MetadataKeyAttributes.EVENT_TIMESTAMP_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.dynamodb.converter.MetadataKeyAttributes.PARTITION_KEY_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.dynamodb.converter.MetadataKeyAttributes.SORT_KEY_METADATA_ATTRIBUTE;
import static org.opensearch.dataprepper.plugins.source.dynamodb.converter.MetadataKeyAttributes.STREAM_EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE;

/**
 * Base Record Processor definition.
 * The record processor is to transform the source data into a JacksonEvent,
 * and then write to buffer.
 */
public abstract class RecordConverter {

    private static final String DEFAULT_ACTION = OpenSearchBulkActions.INDEX.toString();

    private static final int DEFAULT_WRITE_TIMEOUT_MILLIS = 60_000;

    private final Buffer<Record<Event>> buffer;

    private final TableInfo tableInfo;

    public RecordConverter(Buffer<Record<Event>> buffer, TableInfo tableInfo) {
        this.buffer = buffer;
        this.tableInfo = tableInfo;
    }


    abstract String getEventType();

    /**
     * Default method to conduct the document ID value,
     * Using partition key plus sort key (if any)
     */
    String getId(Map<String, Object> data) {
        String partitionKey = String.valueOf(data.get(tableInfo.getMetadata().getPartitionKeyAttributeName()));
        if (tableInfo.getMetadata().getSortKeyAttributeName() == null) {
            return partitionKey;
        }
        String sortKey = String.valueOf(data.get(tableInfo.getMetadata().getSortKeyAttributeName()));
        return partitionKey + "_" + sortKey;
    }

    String getPartitionKey(final Map<String, Object> data) {
        return String.valueOf(data.get(tableInfo.getMetadata().getPartitionKeyAttributeName()));
    }

    String getSortKey(final Map<String, Object> data) {
        return String.valueOf(data.get(tableInfo.getMetadata().getSortKeyAttributeName()));
    }

    void writeEventsToBuffer(List<Record<Event>> events) throws Exception {
        buffer.writeAll(events, DEFAULT_WRITE_TIMEOUT_MILLIS);
    }

    public Record<Event> convertToEvent(Map<String, Object> data, Instant eventCreationTime, String streamEventName) {
        Event event;
        event = JacksonEvent.builder()
                .withEventType(getEventType())
                .withData(data)
                .build();
        EventMetadata eventMetadata = event.getMetadata();

        eventMetadata.setAttribute(EVENT_TABLE_NAME_METADATA_ATTRIBUTE, tableInfo.getTableName());
        if (eventCreationTime != null) {
            eventMetadata.setAttribute(EVENT_TIMESTAMP_METADATA_ATTRIBUTE, eventCreationTime.toEpochMilli());
        }

        eventMetadata.setAttribute(STREAM_EVENT_NAME_BULK_ACTION_METADATA_ATTRIBUTE, mapStreamEventNameToBulkAction(streamEventName));
        eventMetadata.setAttribute(PRIMARY_KEY_DOCUMENT_ID_METADATA_ATTRIBUTE, getId(data));
        eventMetadata.setAttribute(PARTITION_KEY_METADATA_ATTRIBUTE, getPartitionKey(data));
        eventMetadata.setAttribute(SORT_KEY_METADATA_ATTRIBUTE, getSortKey(data));

        return new Record<>(event);
    }

    public Record<Event> convertToEvent(Map<String, Object> data) {
        return convertToEvent(data, null, null);
    }

    private String mapStreamEventNameToBulkAction(final String streamEventName) {
        if (streamEventName == null) {
            return DEFAULT_ACTION;
        }

        switch (streamEventName) {
            case "INSERT":
                return OpenSearchBulkActions.CREATE.toString();
            case "MODIFY":
                return OpenSearchBulkActions.UPSERT.toString();
            case "REMOVE":
                return OpenSearchBulkActions.DELETE.toString();
            default:
                return DEFAULT_ACTION;
        }
    }

}
