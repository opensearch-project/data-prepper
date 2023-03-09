/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package plugins.codec.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
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
 * An implementation of {@link InputCodec} which parses avro records into fields.
 */
@DataPrepperPlugin(name="avro", pluginType = InputCodec.class)
public class AvroInputCodec implements InputCodec {
    private static final String MESSAGE_FIELD_NAME = "message";
    private static final String FILE_NAME = "avro-data";
    private static final String FILE_SUFFIX = ".avro";
    private static final int EMPTY_INPUT_STREAM_VALUE = 0;

    private static final Logger LOG =  LoggerFactory.getLogger(AvroInputCodec.class);

    @Override
    public void parse(InputStream inputStream, Consumer<Record<Event>> eventConsumer) throws IOException {


        Objects.requireNonNull(inputStream);
        Objects.requireNonNull(eventConsumer);

        if(!(inputStream.available() == EMPTY_INPUT_STREAM_VALUE))
            parseAvroStream(inputStream, eventConsumer);

    }

    private void parseAvroStream(final InputStream inputStream, final Consumer<Record<Event>> eventConsumer)throws IOException {

        File tempFile = File.createTempFile(FILE_NAME, FILE_SUFFIX);

        Files.copy(inputStream, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

        try {

            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
            DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(tempFile, datumReader);
            Schema schema = dataFileReader.getSchema();
            GenericDatumReader<GenericRecord> datumReader1 = new GenericDatumReader<>(schema);

            FileReader<GenericRecord> fileReader = DataFileReader.openReader(tempFile, datumReader1);

            final Map<String, String> eventData = new HashMap<>();

            for (GenericRecord record : fileReader) {

                eventData.put(MESSAGE_FIELD_NAME, record.toString());
                final Event event = JacksonLog.builder().withData(eventData).build();
                eventConsumer.accept(new Record<>(event));

            }

            fileReader.close();

        }
        catch (Exception avroException){
            LOG.error("An exception has occurred while parsing avro InputStream ", avroException);
        }
        finally {
            tempFile.delete();
        }
    }

}