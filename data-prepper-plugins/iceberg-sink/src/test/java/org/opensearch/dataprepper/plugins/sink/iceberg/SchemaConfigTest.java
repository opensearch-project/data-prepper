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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaConfigTest {

    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

    private SchemaConfig parse(final String yaml) throws Exception {
        return YAML.readValue(yaml, SchemaConfig.class);
    }

    @Test
    void toIcebergSchema_basic_columns() throws Exception {
        final SchemaConfig config = parse(
                "columns:\n"
              + "  - name: id\n"
              + "    type: long\n"
              + "    required: true\n"
              + "  - name: name\n"
              + "    type: string\n");
        final Schema schema = config.toIcebergSchema();
        assertEquals(2, schema.columns().size());
        assertTrue(schema.findField("id").isRequired());
        assertFalse(schema.findField("name").isRequired());
        assertEquals(Types.LongType.get(), schema.findField("id").type());
        assertEquals(Types.StringType.get(), schema.findField("name").type());
    }

    @Test
    void toPartitionSpec_unpartitioned_when_null() throws Exception {
        final SchemaConfig config = parse("columns:\n  - name: id\n    type: long\n");
        final Schema schema = config.toIcebergSchema();
        assertEquals(PartitionSpec.unpartitioned(), config.toPartitionSpec(schema));
    }

    @Test
    void toPartitionSpec_identity() throws Exception {
        final SchemaConfig config = parse(
                "columns:\n  - name: id\n    type: long\n"
              + "partition_spec:\n  - column: id\n");
        final Schema schema = config.toIcebergSchema();
        final PartitionSpec spec = config.toPartitionSpec(schema);
        assertEquals(1, spec.fields().size());
        assertEquals("identity", spec.fields().get(0).transform().toString());
    }

    @Test
    void toPartitionSpec_time_transforms() throws Exception {
        final SchemaConfig config = parse(
                "columns:\n"
              + "  - name: ts\n"
              + "    type: timestamp\n"
              + "partition_spec:\n"
              + "  - column: ts\n"
              + "    transform: year\n");
        final Schema schema = config.toIcebergSchema();
        assertEquals("year", config.toPartitionSpec(schema).fields().get(0).transform().toString());
    }

    @Test
    void toPartitionSpec_bucket() throws Exception {
        final SchemaConfig config = parse(
                "columns:\n  - name: id\n    type: long\n"
              + "partition_spec:\n  - column: id\n    transform: bucket[16]\n");
        final Schema schema = config.toIcebergSchema();
        final PartitionSpec spec = config.toPartitionSpec(schema);
        assertEquals("bucket[16]", spec.fields().get(0).transform().toString());
    }

    @Test
    void toPartitionSpec_truncate() throws Exception {
        final SchemaConfig config = parse(
                "columns:\n  - name: name\n    type: string\n"
              + "partition_spec:\n  - column: name\n    transform: truncate[4]\n");
        final Schema schema = config.toIcebergSchema();
        final PartitionSpec spec = config.toPartitionSpec(schema);
        assertEquals("truncate[4]", spec.fields().get(0).transform().toString());
    }

    @Test
    void toPartitionSpec_unsupported_transform_throws() throws Exception {
        final SchemaConfig config = parse(
                "columns:\n  - name: id\n    type: long\n"
              + "partition_spec:\n  - column: id\n    transform: unknown\n");
        final Schema schema = config.toIcebergSchema();
        assertThrows(IllegalArgumentException.class, () -> config.toPartitionSpec(schema));
    }
}
