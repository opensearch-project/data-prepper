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

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.avro.PlannedDataReader;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Serializes and deserializes Iceberg {@link Record}s using Avro binary encoding.
 * Uses Iceberg's {@link DataWriter} and {@link PlannedDataReader} which handle
 * Iceberg-specific types (OffsetDateTime, LocalDate, etc.) correctly.
 */
public class RecordAvroSerializer {

    private RecordAvroSerializer() {}

    public static byte[] serialize(final Record record, final org.apache.avro.Schema avroSchema) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        final DataWriter<Record> writer = DataWriter.create(avroSchema);
        writer.write(record, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    public static Record deserialize(final byte[] data, final Schema icebergSchema,
                                     final org.apache.avro.Schema avroSchema) throws IOException {
        final BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
        final PlannedDataReader<Record> reader = PlannedDataReader.create(icebergSchema);
        reader.setSchema(avroSchema);
        return reader.read(null, decoder);
    }
}
