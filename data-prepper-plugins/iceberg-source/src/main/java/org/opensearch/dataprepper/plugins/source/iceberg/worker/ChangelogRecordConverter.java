/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.iceberg.worker;

import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Converts an Iceberg Record to a Data Prepper Event.
 */
public class ChangelogRecordConverter {

    private static final String EVENT_TYPE = "EVENT";

    private final String tableName;
    private final List<String> identifierColumns;

    public ChangelogRecordConverter(final String tableName, final List<String> identifierColumns) {
        this.tableName = tableName;
        this.identifierColumns = identifierColumns;
    }

    public Event convert(final Record record,
                         final org.apache.iceberg.Schema schema,
                         final String operation,
                         final long snapshotId) {
        final Map<String, Object> data = new LinkedHashMap<>();
        for (final Types.NestedField field : schema.columns()) {
            final Object value = record.getField(field.name());
            data.put(field.name(), convertValue(value));
        }

        final Event event = JacksonEvent.builder()
                .withEventType(EVENT_TYPE)
                .withData(data)
                .build();

        event.getMetadata().setAttribute("iceberg_operation", operation);
        event.getMetadata().setAttribute("iceberg_table_name", tableName);
        event.getMetadata().setAttribute("iceberg_snapshot_id", snapshotId);
        event.getMetadata().setAttribute("bulk_action", toBulkAction(operation));

        if (!identifierColumns.isEmpty()) {
            final String documentId = identifierColumns.stream()
                    .map(col -> String.valueOf(data.getOrDefault(col, "")))
                    .collect(Collectors.joining("|"));
            event.getMetadata().setAttribute("document_id", documentId);
        }

        return event;
    }

    private String toBulkAction(final String operation) {
        if ("DELETE".equals(operation)) {
            return "delete";
        }
        return "index";
    }

    private Object convertValue(final Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof BigDecimal) {
            return ((BigDecimal) value).toPlainString();
        }
        if (value instanceof ByteBuffer) {
            final ByteBuffer buf = (ByteBuffer) value;
            final byte[] bytes = new byte[buf.remaining()];
            buf.duplicate().get(bytes);
            return Base64.getEncoder().encodeToString(bytes);
        }
        if (value instanceof byte[]) {
            return Base64.getEncoder().encodeToString((byte[]) value);
        }
        if (value instanceof LocalDate) {
            return value.toString();
        }
        if (value instanceof LocalTime) {
            return value.toString();
        }
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        }
        if (value instanceof OffsetDateTime) {
            return ((OffsetDateTime) value).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        }
        if (value instanceof UUID) {
            return value.toString();
        }
        if (value instanceof Record) {
            final Record struct = (Record) value;
            final Map<String, Object> map = new LinkedHashMap<>();
            for (int i = 0; i < struct.size(); i++) {
                map.put("_" + i, convertValue(struct.get(i)));
            }
            return map;
        }
        if (value instanceof List) {
            return ((List<?>) value).stream()
                    .map(this::convertValue)
                    .collect(Collectors.toList());
        }
        if (value instanceof Map) {
            final Map<?, ?> srcMap = (Map<?, ?>) value;
            final Map<String, Object> result = new LinkedHashMap<>();
            srcMap.forEach((k, v) -> result.put(String.valueOf(k), convertValue(v)));
            return result;
        }
        return value;
    }
}
