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
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.PhysicalType;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantArray;
import org.apache.iceberg.variants.VariantObject;
import org.apache.iceberg.variants.VariantPrimitive;
import org.apache.iceberg.variants.VariantValue;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ChangelogRecordConverterTest {

    private static final Schema TEST_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "age", Types.IntegerType.get())
    );

    @Test
    void convert_insertOperation_setsCorrectMetadata() {
        final ChangelogRecordConverter converter = new ChangelogRecordConverter("test_table", List.of("id"));
        final Record record = GenericRecord.create(TEST_SCHEMA);
        record.setField("id", 1);
        record.setField("name", "Alice");
        record.setField("age", 30);

        final Event event = converter.convert(record, TEST_SCHEMA, "INSERT", 12345L);

        assertThat(event.get("id", Integer.class), equalTo(1));
        assertThat(event.get("name", String.class), equalTo("Alice"));
        assertThat(event.get("age", Integer.class), equalTo(30));
        assertThat(event.getMetadata().getAttribute("iceberg_operation"), equalTo("INSERT"));
        assertThat(event.getMetadata().getAttribute("iceberg_table_name"), equalTo("test_table"));
        assertThat(event.getMetadata().getAttribute("iceberg_snapshot_id"), equalTo(12345L));
        assertThat(event.getMetadata().getAttribute("bulk_action"), equalTo("index"));
        assertThat(event.getMetadata().getAttribute("document_id"), equalTo("1"));
    }

    @Test
    void convert_deleteOperation_setsDeleteBulkAction() {
        final ChangelogRecordConverter converter = new ChangelogRecordConverter("test_table", List.of("id"));
        final Record record = GenericRecord.create(TEST_SCHEMA);
        record.setField("id", 1);
        record.setField("name", "Alice");
        record.setField("age", 30);

        final Event event = converter.convert(record, TEST_SCHEMA, "DELETE", 12345L);

        assertThat(event.getMetadata().getAttribute("bulk_action"), equalTo("delete"));
        assertThat(event.getMetadata().getAttribute("document_id"), equalTo("1"));
    }

    @Test
    void convert_multipleIdentifierColumns_concatenatedWithPipe() {
        final ChangelogRecordConverter converter = new ChangelogRecordConverter("test_table", List.of("id", "name"));
        final Record record = GenericRecord.create(TEST_SCHEMA);
        record.setField("id", 1);
        record.setField("name", "Alice");
        record.setField("age", 30);

        final Event event = converter.convert(record, TEST_SCHEMA, "INSERT", 12345L);

        assertThat(event.getMetadata().getAttribute("document_id"), equalTo("1|Alice"));
    }

    @Test
    void convert_noIdentifierColumns_noDocumentId() {
        final ChangelogRecordConverter converter = new ChangelogRecordConverter("test_table", List.of());
        final Record record = GenericRecord.create(TEST_SCHEMA);
        record.setField("id", 1);
        record.setField("name", "Alice");
        record.setField("age", 30);

        final Event event = converter.convert(record, TEST_SCHEMA, "INSERT", 12345L);

        assertThat(event.getMetadata().getAttribute("document_id"), nullValue());
    }

    @Test
    void convert_decimalType_convertedToString() {
        final Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "price", Types.DecimalType.of(10, 2))
        );
        final ChangelogRecordConverter converter = new ChangelogRecordConverter("test_table", List.of("id"));
        final Record record = GenericRecord.create(schema);
        record.setField("id", 1);
        record.setField("price", new BigDecimal("123.45"));

        final Event event = converter.convert(record, schema, "INSERT", 12345L);

        assertThat(event.get("price", String.class), equalTo("123.45"));
    }

    @Test
    void convert_dateType_convertedToIsoString() {
        final Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "created", Types.DateType.get())
        );
        final ChangelogRecordConverter converter = new ChangelogRecordConverter("test_table", List.of("id"));
        final Record record = GenericRecord.create(schema);
        record.setField("id", 1);
        record.setField("created", LocalDate.of(2024, 1, 15));

        final Event event = converter.convert(record, schema, "INSERT", 12345L);

        assertThat(event.get("created", String.class), equalTo("2024-01-15"));
    }

    @Test
    void convert_nullField_preservedAsNull() {
        final ChangelogRecordConverter converter = new ChangelogRecordConverter("test_table", List.of("id"));
        final Record record = GenericRecord.create(TEST_SCHEMA);
        record.setField("id", 1);
        record.setField("name", null);
        record.setField("age", null);

        final Event event = converter.convert(record, TEST_SCHEMA, "INSERT", 12345L);

        assertThat(event.get("id", Integer.class), equalTo(1));
        assertThat(event.get("name", Object.class), nullValue());
    }

    @Test
    void convert_structType_preservesFieldNames() {
        final Types.StructType addressType = Types.StructType.of(
                Types.NestedField.required(10, "city", Types.StringType.get()),
                Types.NestedField.optional(11, "zip", Types.IntegerType.get())
        );
        final Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "address", addressType)
        );
        final Record addressRecord = GenericRecord.create(addressType);
        addressRecord.setField("city", "Tokyo");
        addressRecord.setField("zip", 100);
        final Record record = GenericRecord.create(schema);
        record.setField("id", 1);
        record.setField("address", addressRecord);

        final ChangelogRecordConverter converter = new ChangelogRecordConverter("test_table", List.of("id"));
        final Event event = converter.convert(record, schema, "INSERT", 12345L);

        @SuppressWarnings("unchecked")
        final Map<String, Object> address = (Map<String, Object>) event.get("address", Object.class);
        assertThat(address.get("city"), equalTo("Tokyo"));
        assertThat(address.get("zip"), equalTo(100));
    }

    @Test
    void convert_variantType_objectConvertedToMap() {
        final Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "data", Types.VariantType.get())
        );

        final VariantPrimitive<?> cityValue = mock(VariantPrimitive.class);
        when(cityValue.type()).thenReturn(PhysicalType.STRING);
        doReturn("Tokyo").when(cityValue).get();
        doReturn(cityValue).when(cityValue).asPrimitive();

        final VariantObject variantObj = mock(VariantObject.class);
        when(variantObj.type()).thenReturn(PhysicalType.OBJECT);
        when(variantObj.fieldNames()).thenReturn(List.of("city"));
        when(variantObj.get("city")).thenReturn(cityValue);
        when(variantObj.asObject()).thenReturn(variantObj);

        final Variant variant = mock(Variant.class);
        when(variant.value()).thenReturn(variantObj);

        final Record record = GenericRecord.create(schema);
        record.setField("id", 1);
        record.setField("data", variant);

        final ChangelogRecordConverter converter = new ChangelogRecordConverter("test_table", List.of("id"));
        final Event event = converter.convert(record, schema, "INSERT", 12345L);

        @SuppressWarnings("unchecked")
        final Map<String, Object> data = (Map<String, Object>) event.get("data", Object.class);
        assertThat(data.get("city"), equalTo("Tokyo"));
    }

    @Test
    void convert_variantType_arrayConvertedToList() {
        final Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "tags", Types.VariantType.get())
        );

        final VariantPrimitive<?> elem0 = mock(VariantPrimitive.class);
        when(elem0.type()).thenReturn(PhysicalType.STRING);
        doReturn("a").when(elem0).get();
        doReturn(elem0).when(elem0).asPrimitive();
        final VariantPrimitive<?> elem1 = mock(VariantPrimitive.class);
        when(elem1.type()).thenReturn(PhysicalType.STRING);
        doReturn("b").when(elem1).get();
        doReturn(elem1).when(elem1).asPrimitive();

        final VariantArray variantArr = mock(VariantArray.class);
        when(variantArr.type()).thenReturn(PhysicalType.ARRAY);
        when(variantArr.numElements()).thenReturn(2);
        when(variantArr.get(0)).thenReturn(elem0);
        when(variantArr.get(1)).thenReturn(elem1);
        when(variantArr.asArray()).thenReturn(variantArr);

        final Variant variant = mock(Variant.class);
        when(variant.value()).thenReturn(variantArr);

        final Record record = GenericRecord.create(schema);
        record.setField("id", 1);
        record.setField("tags", variant);

        final ChangelogRecordConverter converter = new ChangelogRecordConverter("test_table", List.of("id"));
        final Event event = converter.convert(record, schema, "INSERT", 12345L);

        @SuppressWarnings("unchecked")
        final List<Object> tags = (List<Object>) event.get("tags", Object.class);
        assertThat(tags.get(0), equalTo("a"));
        assertThat(tags.get(1), equalTo("b"));
    }
}
