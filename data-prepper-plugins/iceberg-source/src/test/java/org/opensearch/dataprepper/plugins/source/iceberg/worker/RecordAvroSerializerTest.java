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

import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class RecordAvroSerializerTest {

    @Test
    void roundtrip_with_temporal_types() throws IOException {
        final Schema icebergSchema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()),
                Types.NestedField.optional(3, "event_date", Types.DateType.get()),
                Types.NestedField.optional(4, "event_time", Types.TimeType.get()),
                Types.NestedField.optional(5, "created_at", Types.TimestampType.withoutZone()),
                Types.NestedField.optional(6, "updated_at", Types.TimestampType.withZone()),
                Types.NestedField.optional(7, "score", Types.DoubleType.get()),
                Types.NestedField.optional(8, "active", Types.BooleanType.get())
        );

        final org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(icebergSchema, "test_table");

        final GenericRecord original = GenericRecord.create(icebergSchema);
        original.setField("id", 42);
        original.setField("name", "Alice");
        original.setField("event_date", LocalDate.of(2024, 3, 15));
        original.setField("event_time", LocalTime.of(14, 30, 0));
        original.setField("created_at", LocalDateTime.of(2024, 3, 15, 14, 30, 0));
        original.setField("updated_at", OffsetDateTime.of(2024, 3, 15, 14, 30, 0, 0, ZoneOffset.UTC));
        original.setField("score", 99.5);
        original.setField("active", true);

        final byte[] serialized = RecordAvroSerializer.serialize(original, avroSchema);
        final Record deserialized = RecordAvroSerializer.deserialize(serialized, icebergSchema, avroSchema);

        assertThat(deserialized.getField("id"), equalTo(42));
        assertThat(deserialized.getField("name"), equalTo("Alice"));
        assertThat(deserialized.getField("event_date"), equalTo(LocalDate.of(2024, 3, 15)));
        assertThat(deserialized.getField("event_time"), equalTo(LocalTime.of(14, 30, 0)));
        assertThat(deserialized.getField("created_at"), equalTo(LocalDateTime.of(2024, 3, 15, 14, 30, 0)));
        assertThat(deserialized.getField("updated_at"), equalTo(OffsetDateTime.of(2024, 3, 15, 14, 30, 0, 0, ZoneOffset.UTC)));
        assertThat(deserialized.getField("score"), equalTo(99.5));
        assertThat(deserialized.getField("active"), equalTo(true));
    }

    @Test
    void roundtrip_with_null_values() throws IOException {
        final Schema icebergSchema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "updated_at", Types.TimestampType.withZone()),
                Types.NestedField.optional(3, "name", Types.StringType.get())
        );

        final org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(icebergSchema, "test_table");

        final GenericRecord original = GenericRecord.create(icebergSchema);
        original.setField("id", 1);
        original.setField("updated_at", null);
        original.setField("name", null);

        final byte[] serialized = RecordAvroSerializer.serialize(original, avroSchema);
        final Record deserialized = RecordAvroSerializer.deserialize(serialized, icebergSchema, avroSchema);

        assertThat(deserialized.getField("id"), equalTo(1));
        assertThat(deserialized.getField("updated_at"), equalTo(null));
        assertThat(deserialized.getField("name"), equalTo(null));
    }
}
