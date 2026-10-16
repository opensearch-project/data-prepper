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
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IcebergSinkConfigTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private IcebergSinkConfig deserialize(final Map<String, Object> map) throws Exception {
        return MAPPER.convertValue(map, IcebergSinkConfig.class);
    }

    @Test
    void deserialize_requiredFieldsOnly() throws Exception {
        final IcebergSinkConfig config = deserialize(Map.of(
                "catalog", Map.of("type", "rest"),
                "table_identifier", "db.table"
        ));

        assertEquals(Map.of("type", "rest"), config.getCatalog());
        assertEquals("db.table", config.getTableIdentifier());
        assertNull(config.getOperation());
        assertTrue(config.getIdentifierColumns().isEmpty());
    }

    @Test
    void deserialize_allFields() throws Exception {
        final IcebergSinkConfig config = deserialize(Map.of(
                "catalog", Map.of("type", "glue", "warehouse", "s3://bucket/wh"),
                "table_identifier", "ns.orders",
                "operation", "${getMetadata(\"action\")}",
                "identifier_columns", List.of("id", "ts")
        ));

        assertEquals("ns.orders", config.getTableIdentifier());
        assertEquals("${getMetadata(\"action\")}", config.getOperation());
        assertEquals(List.of("id", "ts"), config.getIdentifierColumns());
    }

    @Test
    void defaults_commitInterval() {
        final IcebergSinkConfig config = new IcebergSinkConfig();
        assertEquals(Duration.ofMinutes(5), config.getCommitInterval());
    }

    @Test
    void defaults_flushInterval() {
        final IcebergSinkConfig config = new IcebergSinkConfig();
        assertEquals(Duration.ofMinutes(5), config.getFlushInterval());
    }

    @Test
    void identifierColumns_defaultsToEmptyList() throws Exception {
        final IcebergSinkConfig config = deserialize(Map.of(
                "catalog", Map.of("type", "rest"),
                "table_identifier", "db.table"
        ));
        assertEquals(Collections.emptyList(), config.getIdentifierColumns());
    }

    @Test
    void tableProperties_defaultsToEmptyMap() throws Exception {
        final IcebergSinkConfig config = deserialize(Map.of(
                "catalog", Map.of("type", "rest"),
                "table_identifier", "db.table"
        ));
        assertEquals(Collections.emptyMap(), config.getTableProperties());
    }

    @Test
    void tableProperties_deserialized() throws Exception {
        final IcebergSinkConfig config = deserialize(Map.of(
                "catalog", Map.of("type", "rest"),
                "table_identifier", "db.table",
                "table_properties", Map.of("format-version", "3", "write.format.default", "parquet")
        ));
        assertEquals("3", config.getTableProperties().get("format-version"));
        assertEquals("parquet", config.getTableProperties().get("write.format.default"));
    }

    @Test
    void tableLocation_deserialized() throws Exception {
        final IcebergSinkConfig config = deserialize(Map.of(
                "catalog", Map.of("type", "rest"),
                "table_identifier", "db.table",
                "table_location", "s3://bucket/custom/path"
        ));
        assertEquals("s3://bucket/custom/path", config.getTableLocation());
    }

    @Test
    void dlq_defaultsToNull() throws Exception {
        final IcebergSinkConfig config = deserialize(Map.of(
                "catalog", Map.of("type", "rest"),
                "table_identifier", "db.table"
        ));
        assertNull(config.getDlq());
    }

    @Test
    void defaults_ackPollInterval() {
        final IcebergSinkConfig config = new IcebergSinkConfig();
        assertEquals(Duration.ofSeconds(5), config.getAckPollInterval());
    }

    @Test
    void ackPollInterval_customValue() throws Exception {
        final IcebergSinkConfig config = new IcebergSinkConfig();
        ReflectivelySetField.setField(
                IcebergSinkConfig.class, config, "ackPollInterval", Duration.ofSeconds(10));
        assertEquals(Duration.ofSeconds(10), config.getAckPollInterval());
    }
}
