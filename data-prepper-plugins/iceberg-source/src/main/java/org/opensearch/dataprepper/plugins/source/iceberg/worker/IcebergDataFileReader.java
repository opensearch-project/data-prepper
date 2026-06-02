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

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;

/**
 * Reads Iceberg data files in Parquet, Avro, or ORC format.
 */
public class IcebergDataFileReader {

    public CloseableIterable<Record> open(final InputFile inputFile, final Schema schema, final String filePath) {
        final FileFormat format = FileFormat.fromFileName(filePath);
        if (format == null) {
            throw new IllegalArgumentException("Cannot determine file format for: " + filePath);
        }
        switch (format) {
            case PARQUET:
                return Parquet.read(inputFile)
                        .project(schema)
                        .createReaderFunc(fs -> GenericParquetReaders.buildReader(schema, fs))
                        .build();
            case AVRO:
                return Avro.read(inputFile)
                        .project(schema)
                        .createReaderFunc(DataReader::create)
                        .build();
            case ORC:
                return ORC.read(inputFile)
                        .project(schema)
                        .createReaderFunc(fs -> GenericOrcReader.buildReader(schema, fs))
                        .build();
            default:
                throw new UnsupportedOperationException("Unsupported file format: " + format);
        }
    }
}
