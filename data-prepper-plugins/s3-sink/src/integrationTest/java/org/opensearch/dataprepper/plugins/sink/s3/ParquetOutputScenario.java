/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.InputFile;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.plugins.codec.parquet.ParquetOutputCodec;
import org.opensearch.dataprepper.plugins.codec.parquet.ParquetOutputCodecConfig;
import org.opensearch.dataprepper.plugins.fs.LocalInputFile;
import org.opensearch.dataprepper.plugins.sink.s3.accumulator.BufferTypeOptions;
import org.opensearch.dataprepper.plugins.sink.s3.compression.CompressionOption;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class ParquetOutputScenario implements OutputScenario {
    @Override
    public OutputCodec getCodec() {
        return new ParquetOutputCodec(new ParquetOutputCodecConfig());
    }

    @Override
    public boolean isCompressionInternal() {
        return true;
    }

    @Override
    public Set<BufferTypeOptions> getIncompatibleBufferTypes() {
        return Set.of(BufferTypeOptions.LOCALFILE, BufferTypeOptions.MULTI_PART);
    }

    @Override
    public void validate(int expectedRecords, final List<Map<String, Object>> sampleEventData, final File actualContentFile, CompressionScenario compressionScenario) throws IOException {
        final InputFile inputFile = new LocalInputFile(actualContentFile);

        CompressionCodecName expectedCompressionCodec = determineCompressionCodec(compressionScenario.getCompressionOption());
        validateParquetStructure(expectedRecords, sampleEventData, inputFile, expectedCompressionCodec);

        final Map<String, Map<String, Object>> eventsById = sampleEventData.stream()
                .collect(Collectors.toMap(data -> (String) data.get("id"), data -> data));

        int validatedRecords = 0;

        int count = 0;
        try (final ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(inputFile)
                .build()) {
            GenericRecord record;

            while ((record = reader.read()) != null) {
                assertThat(record.getSchema(), notNullValue());

                assertThat(record.hasField("id"), equalTo(true));
                assertThat(record.get("id"), instanceOf(Utf8.class));
                final String id = record.get("id").toString();

                final Map<String, Object> expectedData = eventsById.get(id);
                if(expectedData != null) {
                    for (String key : expectedData.keySet()) {
                        assertThat(record.hasField(key), notNullValue());
                        assertThat(record.get(key), notNullValue());

                        final Object expectedValue = expectedData.get(key);
                        if (expectedValue instanceof String) {
                            assertThat(record.get(key), instanceOf(Utf8.class));
                            assertThat(record.get(key).toString(), equalTo(expectedValue));
                        }
                    }
                    validatedRecords++;
                }

                count++;
            }
        }

        assertThat(count, equalTo(expectedRecords));

        assertThat("Not all the sample data was validated.", validatedRecords, equalTo(sampleEventData.size()));
    }

    private static void validateParquetStructure(int expectedRecords, final List<Map<String, Object>> allEventData, final InputFile inputFile, CompressionCodecName expectedCompressionCodec) throws IOException {
        // This test assumes that the data all has the same keys.
        final Map<String, Object> sampleEvent = allEventData.iterator().next();

        try (final ParquetFileReader parquetFileReader = new ParquetFileReader(inputFile, ParquetReadOptions.builder().build())) {
            final ParquetMetadata footer = parquetFileReader.getFooter();

            assertThat(footer, notNullValue());
            assertThat(footer.getFileMetaData(), notNullValue());

            assertThat(parquetFileReader.getRecordCount(), equalTo((long) expectedRecords));
            assertThat(parquetFileReader.getFileMetaData().getSchema(), notNullValue());

            assertThat(parquetFileReader.getRowGroups(), notNullValue());
            assertThat(parquetFileReader.getRowGroups().size(), greaterThanOrEqualTo(1));

            for (BlockMetaData rowGroup : parquetFileReader.getRowGroups()) {
                assertThat(rowGroup.getColumns(), notNullValue());
                assertThat(rowGroup.getColumns().size(), equalTo(sampleEvent.keySet().size()));
                for (ColumnChunkMetaData column : rowGroup.getColumns()) {
                    assertThat(column.getCodec(), equalTo(expectedCompressionCodec));
                }

            }
        }
    }

    @Override
    public String toString() {
        return "Parquet";
    }

    private static CompressionCodecName determineCompressionCodec(CompressionOption compressionOption) {
        switch (compressionOption) {
            case NONE:
                return CompressionCodecName.UNCOMPRESSED;
            case GZIP:
                return CompressionCodecName.GZIP;
            case SNAPPY:
                return CompressionCodecName.SNAPPY;
        }

        throw new RuntimeException("The provided compression option is not supported by Parquet or is not configured for testing: " + compressionOption.getOption());
    }
}
