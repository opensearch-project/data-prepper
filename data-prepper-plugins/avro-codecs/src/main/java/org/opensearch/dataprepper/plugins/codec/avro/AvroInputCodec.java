/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

import org.apache.avro.file.DataFileStream;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;



/**
 * An implementation of {@link InputCodec} which parses avro records into fields.
 */
@DataPrepperPlugin(name="avro", pluginType = InputCodec.class)
public class AvroInputCodec implements InputCodec {

    private static final Logger LOG =  LoggerFactory.getLogger(AvroInputCodec.class);

    @Override
    public void parse(InputStream inputStream, Consumer<Record<Event>> eventConsumer) throws IOException {


        Objects.requireNonNull(inputStream);
        Objects.requireNonNull(eventConsumer);
        parseAvroStream(inputStream, eventConsumer);

    }

    private void parseAvroStream(final InputStream inputStream, final Consumer<Record<Event>> eventConsumer)throws IOException {

        try {

            final byte[] avroData=inputStream.readAllBytes();
            ByteArrayInputStream byteArrayInputStream=new ByteArrayInputStream(avroData);
            DataFileStream<GenericRecord> stream = new DataFileStream<GenericRecord>(byteArrayInputStream, new GenericDatumReader<GenericRecord>());
            Schema schema=stream.getSchema();

            while (stream.hasNext()) {

                final Map<String, Object> eventData = new HashMap<>();

                GenericRecord record= stream.next();
                for(Schema.Field field : schema.getFields()) {

                    eventData.put(field.name(), record.get(field.name()));

                }
                final Event event = JacksonLog.builder().withData(eventData).build();
                eventConsumer.accept(new Record<>(event));

            }

        }
        catch (Exception avroException){
            LOG.error("An exception has occurred while parsing avro InputStream ", avroException);
        }
    }

}