/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * An implementation of {@link InputCodec} which parses parquet records into fields.
 */
@DataPrepperPlugin(name = "parquet", pluginType = InputCodec.class)
public class ParquetInputCodec implements InputCodec {

    private static final String MESSAGE_FIELD_NAME = "message";
    private static final String FILE_NAME = "parquet-data";
    private static final String FILE_SUFFIX = ".parquet";
    private static final int EMPTY_INPUT_STREAM_VALUE = 0;
    private static final Logger LOG = LoggerFactory.getLogger(ParquetInputCodec.class);

    @Override
    public void parse(InputStream inputStream, Consumer<Record<Event>> eventConsumer) throws IOException {

        Objects.requireNonNull(inputStream);
        Objects.requireNonNull(eventConsumer);
        parseParquetStream(inputStream, eventConsumer);

    }

    private void parseParquetStream(final InputStream inputStream, final Consumer<Record<Event>> eventConsumer) throws IOException {

        final File tempFile = File.createTempFile(FILE_NAME, FILE_SUFFIX);
        Files.copy(inputStream, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

        try (ParquetFileReader parquetFileReader = new ParquetFileReader(HadoopInputFile.fromPath(new Path(tempFile.toURI()), new Configuration()), ParquetReadOptions.builder().build())) {
            final ParquetMetadata footer = parquetFileReader.getFooter();
            final MessageType schema = createdParquetSchema(footer);

            PageReadStore pages;

            while ((pages = parquetFileReader.readNextRowGroup()) != null) {
                final long rows = pages.getRowCount();
                final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                final RecordReader recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

                final Map<String, String> eventData = new HashMap<>();

                for (int row = 0; row < rows; row++) {
                    final SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
                    eventData.put(MESSAGE_FIELD_NAME, simpleGroup.toString());
                    final Event event = JacksonLog.builder().withData(eventData).build();
                    eventConsumer.accept(new Record<>(event));
                }
            }
        } catch (Exception parquetException) {
            LOG.error("An exception occurred while parsing parquet InputStream  ", parquetException);
        } finally {
            Files.delete(tempFile.toPath());
        }
    }

    private MessageType createdParquetSchema(ParquetMetadata parquetMetadata) {
        return parquetMetadata.getFileMetaData().getSchema();
    }

}
