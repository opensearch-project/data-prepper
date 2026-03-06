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
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

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
}
