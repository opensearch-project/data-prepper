/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.io.InputFile;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.fs.LocalInputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * An implementation of {@link InputCodec} which parses parquet records into fields.
 */
@DataPrepperPlugin(name = "parquet", pluginType = InputCodec.class)
public class ParquetInputCodec implements InputCodec {

    static final String EVENT_TYPE = "event";

    static final String FILE_PREFIX = "parquet-data";

    static final String FILE_SUFFIX = ".parquet";

    private static final Logger LOG = LoggerFactory.getLogger(ParquetInputCodec.class);

    @Override
    public void parse(final InputStream inputStream, final Consumer<Record<Event>> eventConsumer) throws IOException {
        Objects.requireNonNull(inputStream);
        Objects.requireNonNull(eventConsumer);

        final File tempFile = File.createTempFile(FILE_PREFIX, FILE_SUFFIX);
        Files.copy(inputStream, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

        try {
            parseParquetFile(new LocalInputFile(tempFile), eventConsumer);
        } finally {
            Files.delete(tempFile.toPath());
        }

    }

    @Override
    public void parse(final InputFile inputFile, final Consumer<Record<Event>> eventConsumer) throws IOException {
        Objects.requireNonNull(inputFile);
        Objects.requireNonNull(eventConsumer);
        parseParquetFile(inputFile, eventConsumer);
    }

    private void parseParquetFile(final InputFile inputFile, final Consumer<Record<Event>> eventConsumer) throws IOException {

        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(inputFile)
                .build()) {
            GenericRecord record;

            while ((record = reader.read()) != null) {
                Schema schema = record.getSchema();
                String json = record.toString();
                Decoder decoder = DecoderFactory.get().jsonDecoder(schema, json);
                DatumReader<GenericData.Record> datumReader = new GenericDatumReader<>(schema);
                GenericRecord genericRecord = datumReader.read(null, decoder);

                final JacksonEvent event = JacksonEvent.builder()
                        .withEventType(EVENT_TYPE)
                        .withData(genericRecord.toString())
                        .build();

                eventConsumer.accept(new Record<>(event));
            }
        } catch (Exception parquetException){
            LOG.error("An exception occurred while parsing parquet InputStream  ", parquetException);
        }
    }

}
