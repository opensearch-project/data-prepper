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
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantArray;
import org.apache.iceberg.variants.VariantObject;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.PhysicalType;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventBuilder;
import org.opensearch.dataprepper.model.event.EventFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
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
    private final EventFactory eventFactory;

    public ChangelogRecordConverter(final String tableName, final List<String> identifierColumns, final EventFactory eventFactory) {
        this.tableName = tableName;
        this.identifierColumns = identifierColumns;
        this.eventFactory = eventFactory;
    }

    public Event convert(final Record record,
                         final org.apache.iceberg.Schema schema,
                         final String operation,
                         final long snapshotId) {
        final Map<String, Object> data = new LinkedHashMap<>();
        for (final Types.NestedField field : schema.columns()) {
            final Object value = record.getField(field.name());
            data.put(field.name(), convertValue(value, field.type()));
        }

        final Event event = eventFactory.eventBuilder(EventBuilder.class)
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

    private Object convertValue(final Object value, final Type type) {
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
        if (value instanceof Record && type instanceof Types.StructType) {
            final Record struct = (Record) value;
            final Types.StructType structType = (Types.StructType) type;
            final Map<String, Object> map = new LinkedHashMap<>();
            for (final Types.NestedField field : structType.fields()) {
                map.put(field.name(), convertValue(struct.getField(field.name()), field.type()));
            }
            return map;
        }
        if (value instanceof List && type instanceof Types.ListType) {
            final Types.ListType listType = (Types.ListType) type;
            return ((List<?>) value).stream()
                    .map(v -> convertValue(v, listType.elementType()))
                    .collect(Collectors.toList());
        }
        if (value instanceof Map && type instanceof Types.MapType) {
            final Types.MapType mapType = (Types.MapType) type;
            final Map<?, ?> srcMap = (Map<?, ?>) value;
            final Map<String, Object> result = new LinkedHashMap<>();
            srcMap.forEach((k, v) -> result.put(String.valueOf(k), convertValue(v, mapType.valueType())));
            return result;
        }
        if (value instanceof Variant) {
            return convertVariant((Variant) value);
        }
        return value;
    }

    private Object convertVariant(final Variant variant) {
        return convertVariantValue(variant.value());
    }

    private Object convertVariantValue(final VariantValue value) {
        final PhysicalType physicalType = value.type();
        if (physicalType == PhysicalType.OBJECT) {
            final VariantObject obj = value.asObject();
            final Map<String, Object> map = new LinkedHashMap<>();
            for (final String fieldName : obj.fieldNames()) {
                map.put(fieldName, convertVariantValue(obj.get(fieldName)));
            }
            return map;
        }
        if (physicalType == PhysicalType.ARRAY) {
            final VariantArray arr = value.asArray();
            final List<Object> list = new ArrayList<>();
            for (int i = 0; i < arr.numElements(); i++) {
                list.add(convertVariantValue(arr.get(i)));
            }
            return list;
        }
        return value.asPrimitive().get();
    }
}
