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
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class SchemaInferenceTest {

    @Test
    void infer_string() {
        final Schema schema = SchemaInference.infer(Map.of("name", "alice"));
        assertEquals(Types.StringType.get(), schema.findField("name").type());
        assertFalse(schema.findField("name").isRequired());
    }

    @Test
    void infer_boolean() {
        final Schema schema = SchemaInference.infer(Map.of("flag", true));
        assertEquals(Types.BooleanType.get(), schema.findField("flag").type());
    }

    @Test
    void infer_integer_and_long() {
        final Schema schema = SchemaInference.infer(Map.of("a", 42, "b", 100L));
        assertEquals(Types.LongType.get(), schema.findField("a").type());
        assertEquals(Types.LongType.get(), schema.findField("b").type());
    }

    @Test
    void infer_float_and_double() {
        final Schema schema = SchemaInference.infer(Map.of("a", 1.5f, "b", 2.5d));
        assertEquals(Types.DoubleType.get(), schema.findField("a").type());
        assertEquals(Types.DoubleType.get(), schema.findField("b").type());
    }

    @Test
    void infer_bigdecimal() {
        final Schema schema = SchemaInference.infer(Map.of("price", new BigDecimal("19.99")));
        final Types.DecimalType type = (Types.DecimalType) schema.findField("price").type();
        assertEquals(4, type.precision());
        assertEquals(2, type.scale());
    }

    @Test
    void infer_null_defaults_to_string() {
        final Map<String, Object> data = new LinkedHashMap<>();
        data.put("x", null);
        final Schema schema = SchemaInference.infer(data);
        assertEquals(Types.StringType.get(), schema.findField("x").type());
    }

    @Test
    void infer_nested_map_as_struct() {
        final Map<String, Object> nested = Map.of("city", "tokyo", "zip", 100);
        final Schema schema = SchemaInference.infer(Map.of("address", nested));
        final Types.StructType struct = schema.findField("address").type().asStructType();
        assertEquals(Types.StringType.get(), struct.field("city").type());
        assertEquals(Types.LongType.get(), struct.field("zip").type());
    }

    @Test
    void infer_empty_map_defaults_to_string() {
        final Schema schema = SchemaInference.infer(Map.of("empty", Collections.emptyMap()));
        assertEquals(Types.StringType.get(), schema.findField("empty").type());
    }

    @Test
    void infer_list() {
        final Schema schema = SchemaInference.infer(Map.of("tags", List.of("a", "b")));
        final Types.ListType listType = schema.findField("tags").type().asListType();
        assertEquals(Types.StringType.get(), listType.elementType());
    }

    @Test
    void infer_empty_list_defaults_to_string() {
        final Schema schema = SchemaInference.infer(Map.of("empty", Collections.emptyList()));
        assertEquals(Types.StringType.get(), schema.findField("empty").type());
    }

    @Test
    void infer_list_of_integers() {
        final Schema schema = SchemaInference.infer(Map.of("ids", List.of(1, 2, 3)));
        final Types.ListType listType = schema.findField("ids").type().asListType();
        assertEquals(Types.LongType.get(), listType.elementType());
    }
}
