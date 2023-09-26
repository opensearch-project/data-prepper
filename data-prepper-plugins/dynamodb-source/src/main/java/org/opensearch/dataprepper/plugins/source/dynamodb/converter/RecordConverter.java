/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.converter;

import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableInfo;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Base Record Processor definition.
 * The record processor is to transform the source data into a JacksonEvent,
 * and then write to buffer.
 */
public abstract class RecordConverter {


    private static final String KEYS_TAG_NAME = "_id";

    private static final String EVENT_TIMESTAMP_TAG_NAME = "ts";

    private static final String EVENT_OP_TAG_NAME = "op";

    private static final String EVENT_SOURCE_TAG_NAME = "source";

    private static final String DEFAULT_ACTION = "index";

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
        String result;
        String partitionKey = String.valueOf(data.get(tableInfo.getMetadata().getPartitionKeyAttributeName()));
        if (tableInfo.getMetadata().getSortKeyAttributeName() == null) {
            result = partitionKey;
        } else {
            String sortKey = String.valueOf(data.get(tableInfo.getMetadata().getSortKeyAttributeName()));
            return partitionKey + "_" + sortKey;
        }
        return result.replaceAll("\\s", "_");
    }

    void writeEventsToBuffer(List<Record<Event>> events) throws Exception {
        buffer.writeAll(events, DEFAULT_WRITE_TIMEOUT_MILLIS);
    }

    public Record<Event> convertToEvent(Map<String, Object> data, Instant eventCreationTime, String action) {
        Event event;
        event = JacksonEvent.builder()
                .withEventType(getEventType())
                .withData(data)
                .build();
        EventMetadata eventMetadata = event.getMetadata();

        eventMetadata.setAttribute(EVENT_SOURCE_TAG_NAME, tableInfo.getTableArn());
        if (eventCreationTime != null) {
            eventMetadata.setAttribute(EVENT_TIMESTAMP_TAG_NAME, eventCreationTime.toEpochMilli());
        }

        eventMetadata.setAttribute(EVENT_OP_TAG_NAME, action);
        eventMetadata.setAttribute(KEYS_TAG_NAME, getId(data));

        return new Record<>(event);
    }

    public Record<Event> convertToEvent(Map<String, Object> data) {
        return convertToEvent(data, null, DEFAULT_ACTION);
    }


}
