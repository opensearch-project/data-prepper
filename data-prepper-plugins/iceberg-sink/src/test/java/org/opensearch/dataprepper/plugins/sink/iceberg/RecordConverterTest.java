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
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.Variant;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class RecordConverterTest {

    @Test
    void convert_primitiveTypes() {
        final Schema schema = new Schema(
                Types.NestedField.optional(1, "bool_col", Types.BooleanType.get()),
                Types.NestedField.optional(2, "int_col", Types.IntegerType.get()),
                Types.NestedField.optional(3, "long_col", Types.LongType.get()),
                Types.NestedField.optional(4, "float_col", Types.FloatType.get()),
                Types.NestedField.optional(5, "double_col", Types.DoubleType.get()),
                Types.NestedField.optional(6, "string_col", Types.StringType.get())
        );
        final RecordConverter converter = new RecordConverter(schema);

        final Map<String, Object> data = Map.of(
                "bool_col", true,
                "int_col", 42,
                "long_col", 123456789L,
                "float_col", 3.14f,
                "double_col", 2.718,
                "string_col", "hello"
        );

        final GenericRecord record = converter.convert(data);
        assertEquals(true, record.getField("bool_col"));
        assertEquals(42, record.getField("int_col"));
        assertEquals(123456789L, record.getField("long_col"));
        assertEquals(3.14f, record.getField("float_col"));
        assertEquals(2.718, record.getField("double_col"));
        assertEquals("hello", record.getField("string_col"));
    }

    @Test
    void convert_stringCoercion() {
        final Schema schema = new Schema(
                Types.NestedField.optional(1, "int_col", Types.IntegerType.get()),
                Types.NestedField.optional(2, "long_col", Types.LongType.get()),
                Types.NestedField.optional(3, "bool_col", Types.BooleanType.get()),
                Types.NestedField.optional(4, "double_col", Types.DoubleType.get())
        );
        final RecordConverter converter = new RecordConverter(schema);

        final Map<String, Object> data = Map.of(
                "int_col", "42",
                "long_col", "123456789",
                "bool_col", "true",
                "double_col", "2.718"
        );

        final GenericRecord record = converter.convert(data);
        assertEquals(42, record.getField("int_col"));
        assertEquals(123456789L, record.getField("long_col"));
        assertEquals(true, record.getField("bool_col"));
        assertEquals(2.718, record.getField("double_col"));
    }

    @Test
    void convert_decimal() {
        final Schema schema = new Schema(
                Types.NestedField.optional(1, "dec_col", Types.DecimalType.of(10, 2))
        );
        final RecordConverter converter = new RecordConverter(schema);

        final GenericRecord fromNumber = converter.convert(Map.of("dec_col", 123.456));
        assertEquals(new BigDecimal("123.46"), fromNumber.getField("dec_col"));

        final GenericRecord fromString = converter.convert(Map.of("dec_col", "789.12"));
        assertEquals(new BigDecimal("789.12"), fromString.getField("dec_col"));
    }

    @Test
    void convert_date() {
        final Schema schema = new Schema(
                Types.NestedField.optional(1, "date_col", Types.DateType.get())
        );
        final RecordConverter converter = new RecordConverter(schema);

        final GenericRecord fromString = converter.convert(Map.of("date_col", "2024-03-15"));
        assertEquals(LocalDate.of(2024, 3, 15), fromString.getField("date_col"));

        final GenericRecord fromEpoch = converter.convert(Map.of("date_col", 19797));
        assertEquals(LocalDate.ofEpochDay(19797), fromEpoch.getField("date_col"));
    }

    @Test
    void convert_time() {
        final Schema schema = new Schema(
                Types.NestedField.optional(1, "time_col", Types.TimeType.get())
        );
        final RecordConverter converter = new RecordConverter(schema);

        final GenericRecord fromString = converter.convert(Map.of("time_col", "10:30:00"));
        assertEquals(LocalTime.of(10, 30, 0), fromString.getField("time_col"));
    }

    @Test
    void convert_timestamp() {
        final Schema schema = new Schema(
                Types.NestedField.optional(1, "ts_col", Types.TimestampType.withoutZone()),
                Types.NestedField.optional(2, "tstz_col", Types.TimestampType.withZone())
        );
        final RecordConverter converter = new RecordConverter(schema);

        final long epochMillis = 1710500000000L;
        final GenericRecord record = converter.convert(Map.of(
                "ts_col", epochMillis,
                "tstz_col", epochMillis
        ));

        assertInstanceOf(LocalDateTime.class, record.getField("ts_col"));
        assertInstanceOf(OffsetDateTime.class, record.getField("tstz_col"));
    }

    @Test
    void convert_timestampNano() {
        final Schema schema = new Schema(
                Types.NestedField.optional(1, "tsn_col", Types.TimestampNanoType.withoutZone()),
                Types.NestedField.optional(2, "tsntz_col", Types.TimestampNanoType.withZone())
        );
        final RecordConverter converter = new RecordConverter(schema);

        final long epochNanos = 1710500000123456789L;
        final GenericRecord record = converter.convert(Map.of(
                "tsn_col", epochNanos,
                "tsntz_col", epochNanos
        ));

        assertInstanceOf(LocalDateTime.class, record.getField("tsn_col"));
        assertInstanceOf(OffsetDateTime.class, record.getField("tsntz_col"));

        final OffsetDateTime odt = (OffsetDateTime) record.getField("tsntz_col");
        assertEquals(123456789, odt.getNano());
    }

    @Test
    void convert_uuid() {
        final Schema schema = new Schema(
                Types.NestedField.optional(1, "uuid_col", Types.UUIDType.get())
        );
        final RecordConverter converter = new RecordConverter(schema);

        final String uuidStr = "550e8400-e29b-41d4-a716-446655440000";
        final GenericRecord record = converter.convert(Map.of("uuid_col", uuidStr));
        assertEquals(UUID.fromString(uuidStr), record.getField("uuid_col"));
    }

    @Test
    void convert_binary() {
        final Schema schema = new Schema(
                Types.NestedField.optional(1, "bin_col", Types.BinaryType.get())
        );
        final RecordConverter converter = new RecordConverter(schema);

        final byte[] bytes = {1, 2, 3, 4};
        final String base64 = Base64.getEncoder().encodeToString(bytes);
        final GenericRecord record = converter.convert(Map.of("bin_col", base64));
        assertArrayEquals(bytes, ((ByteBuffer) record.getField("bin_col")).array());
    }

    @Test
    void convert_geometry() {
        final Schema schema = new Schema(
                Types.NestedField.optional(1, "geom_col", Types.GeometryType.crs84())
        );
        final RecordConverter converter = new RecordConverter(schema);

        final byte[] wkb = {0, 0, 0, 0, 1};
        final String base64 = Base64.getEncoder().encodeToString(wkb);
        final GenericRecord record = converter.convert(Map.of("geom_col", base64));
        assertNotNull(record.getField("geom_col"));
        assertInstanceOf(ByteBuffer.class, record.getField("geom_col"));
    }

    @Test
    void convert_struct() {
        final Schema schema = new Schema(
                Types.NestedField.optional(1, "nested", Types.StructType.of(
                        Types.NestedField.optional(2, "name", Types.StringType.get()),
                        Types.NestedField.optional(3, "age", Types.IntegerType.get())
                ))
        );
        final RecordConverter converter = new RecordConverter(schema);

        final Map<String, Object> data = Map.of(
                "nested", Map.of("name", "Alice", "age", 30)
        );

        final GenericRecord record = converter.convert(data);
        final GenericRecord nested = (GenericRecord) record.getField("nested");
        assertEquals("Alice", nested.getField("name"));
        assertEquals(30, nested.getField("age"));
    }

    @Test
    void convert_list() {
        final Schema schema = new Schema(
                Types.NestedField.optional(1, "tags", Types.ListType.ofOptional(2, Types.StringType.get()))
        );
        final RecordConverter converter = new RecordConverter(schema);

        final GenericRecord record = converter.convert(Map.of("tags", List.of("a", "b", "c")));
        assertEquals(List.of("a", "b", "c"), record.getField("tags"));
    }

    @Test
    void convert_map() {
        final Schema schema = new Schema(
                Types.NestedField.optional(1, "props", Types.MapType.ofOptional(2, 3, Types.StringType.get(), Types.IntegerType.get()))
        );
        final RecordConverter converter = new RecordConverter(schema);

        final GenericRecord record = converter.convert(Map.of("props", Map.of("x", 1, "y", 2)));
        assertEquals(Map.of("x", 1, "y", 2), record.getField("props"));
    }

    @Test
    void convert_nullValue() {
        final Schema schema = new Schema(
                Types.NestedField.optional(1, "col", Types.StringType.get())
        );
        final RecordConverter converter = new RecordConverter(schema);

        final HashMap<String, Object> data = new HashMap<>();
        data.put("col", null);
        final GenericRecord record = converter.convert(data);
        assertNull(record.getField("col"));
    }

    @Test
    void convert_extraFieldsIgnored() {
        final Schema schema = new Schema(
                Types.NestedField.optional(1, "col", Types.StringType.get())
        );
        final RecordConverter converter = new RecordConverter(schema);

        final GenericRecord record = converter.convert(Map.of("col", "val", "extra", "ignored"));
        assertEquals("val", record.getField("col"));
    }

    @Test
    void convert_missingFieldIsNull() {
        final Schema schema = new Schema(
                Types.NestedField.optional(1, "col", Types.StringType.get())
        );
        final RecordConverter converter = new RecordConverter(schema);

        final GenericRecord record = converter.convert(Map.of());
        assertNull(record.getField("col"));
    }

    @Test
    void convert_variant() {
        final Schema schema = new Schema(
                Types.NestedField.optional(1, "var_col", Types.VariantType.get())
        );
        final RecordConverter converter = new RecordConverter(schema);

        final Map<String, Object> variantData = Map.of(
                "name", "Alice",
                "scores", List.of(95, 87)
        );
        final GenericRecord record = converter.convert(Map.of("var_col", variantData));
        assertNotNull(record.getField("var_col"));
        assertInstanceOf(Variant.class, record.getField("var_col"));
    }

    @Test
    void convert_variantPrimitive() {
        final Schema schema = new Schema(
                Types.NestedField.optional(1, "var_col", Types.VariantType.get())
        );
        final RecordConverter converter = new RecordConverter(schema);

        final GenericRecord record = converter.convert(Map.of("var_col", "hello"));
        assertNotNull(record.getField("var_col"));
        assertInstanceOf(Variant.class, record.getField("var_col"));
    }
}
