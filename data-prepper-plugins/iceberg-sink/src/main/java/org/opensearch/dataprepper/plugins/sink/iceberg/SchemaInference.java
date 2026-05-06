/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.iceberg;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Infers an Iceberg schema from a Java Map (event data).
 * Used when auto_create is enabled and no explicit schema is configured.
 */
public class SchemaInference {

    public static Schema infer(final Map<String, Object> data) {
        final AtomicInteger fieldId = new AtomicInteger(1);
        return new Schema(inferStructFields(data, fieldId));
    }

    private static List<Types.NestedField> inferStructFields(final Map<String, Object> data,
                                                              final AtomicInteger fieldId) {
        final List<Types.NestedField> fields = new ArrayList<>();
        for (final Map.Entry<String, Object> entry : data.entrySet()) {
            fields.add(Types.NestedField.optional(
                    fieldId.getAndIncrement(), entry.getKey(), inferType(entry.getValue(), fieldId)));
        }
        return fields;
    }

    @SuppressWarnings("unchecked")
    static Type inferType(final Object value, final AtomicInteger fieldId) {
        if (value == null) {
            return Types.StringType.get();
        } else if (value instanceof Boolean) {
            return Types.BooleanType.get();
        } else if (value instanceof BigDecimal) {
            final BigDecimal bd = (BigDecimal) value;
            return Types.DecimalType.of(bd.precision(), bd.scale());
        } else if (value instanceof Integer || value instanceof Long) {
            return Types.LongType.get();
        } else if (value instanceof Float || value instanceof Double) {
            return Types.DoubleType.get();
        } else if (value instanceof Map) {
            final Map<String, Object> map = (Map<String, Object>) value;
            if (map.isEmpty()) {
                return Types.StringType.get();
            }
            return Types.StructType.of(inferStructFields(map, fieldId));
        } else if (value instanceof List) {
            final List<?> list = (List<?>) value;
            if (list.isEmpty()) {
                return Types.StringType.get();
            }
            final int elementId = fieldId.getAndIncrement();
            return Types.ListType.ofOptional(elementId, inferType(list.get(0), fieldId));
        } else {
            return Types.StringType.get();
        }
    }
}
