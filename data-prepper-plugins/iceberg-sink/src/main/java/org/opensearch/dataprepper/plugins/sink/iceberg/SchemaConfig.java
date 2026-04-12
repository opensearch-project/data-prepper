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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SchemaConfig {

    @JsonProperty("columns")
    private List<ColumnConfig> columns;

    @JsonProperty("partition_spec")
    private List<PartitionFieldConfig> partitionSpec;

    public Schema toIcebergSchema() {
        final AtomicInteger fieldId = new AtomicInteger(1);
        final List<Types.NestedField> fields = new ArrayList<>();
        for (final ColumnConfig col : columns) {
            final Type type = Types.fromPrimitiveString(col.getType());
            if (col.isRequired()) {
                fields.add(Types.NestedField.required(fieldId.getAndIncrement(), col.getName(), type));
            } else {
                fields.add(Types.NestedField.optional(fieldId.getAndIncrement(), col.getName(), type));
            }
        }
        return new Schema(fields);
    }

    public PartitionSpec toPartitionSpec(final Schema schema) {
        if (partitionSpec == null || partitionSpec.isEmpty()) {
            return PartitionSpec.unpartitioned();
        }
        PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
        for (final PartitionFieldConfig field : partitionSpec) {
            final String transform = field.getTransform();
            final String column = field.getColumn();
            if (transform == null || "identity".equals(transform)) {
                builder.identity(column);
            } else if ("year".equals(transform)) {
                builder.year(column);
            } else if ("month".equals(transform)) {
                builder.month(column);
            } else if ("day".equals(transform)) {
                builder.day(column);
            } else if ("hour".equals(transform)) {
                builder.hour(column);
            } else if (transform.startsWith("bucket")) {
                final int numBuckets = Integer.parseInt(transform.replaceAll("[^0-9]", ""));
                builder.bucket(column, numBuckets);
            } else if (transform.startsWith("truncate")) {
                final int width = Integer.parseInt(transform.replaceAll("[^0-9]", ""));
                builder.truncate(column, width);
            } else {
                throw new IllegalArgumentException("Unsupported partition transform: " + transform);
            }
        }
        return builder.build();
    }

    public static class ColumnConfig {
        @JsonProperty("name")
        private String name;
        @JsonProperty("type")
        private String type;
        @JsonProperty("required")
        private boolean required = false;

        public String getName() { return name; }
        public String getType() { return type; }
        public boolean isRequired() { return required; }
    }

    public static class PartitionFieldConfig {
        @JsonProperty("column")
        private String column;
        @JsonProperty("transform")
        private String transform;

        public String getColumn() { return column; }
        public String getTransform() { return transform; }
    }
}
