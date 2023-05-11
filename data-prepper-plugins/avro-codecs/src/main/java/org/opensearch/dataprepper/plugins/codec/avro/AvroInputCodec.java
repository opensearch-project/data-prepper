/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericFixed;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.util.Utf8;
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

    private void parseAvroStream(final InputStream inputStream, final Consumer<Record<Event>> eventConsumer) {

        try {

            final byte[] avroData=inputStream.readAllBytes();
            ByteArrayInputStream byteArrayInputStream=new ByteArrayInputStream(avroData);
            DataFileStream<GenericRecord> stream = new DataFileStream<GenericRecord>(byteArrayInputStream, new GenericDatumReader<GenericRecord>());
            Schema schema=stream.getSchema();

            while (stream.hasNext()) {

                final Map<String, Object> eventData = new HashMap<>();
                GenericRecord avroRecord= stream.next();

                for(Schema.Field field : schema.getFields()) {
                    Object value=decodeValueIfEncoded(avroRecord.get(field.name()));
                    eventData.put(field.name(), value);

                }
                final Event event = JacksonLog.builder().withData(eventData).build();
                eventConsumer.accept(new Record<>(event));
            }

        }
        catch (Exception avroException){
            LOG.error("An exception has occurred while parsing avro InputStream ", avroException);
        }
    }

    private static Object decodeValueIfEncoded(Object rawValue){
        try{
            if(rawValue instanceof Utf8){
                byte[] utf8Bytes = rawValue.toString().getBytes("UTF-8");
                return new String(utf8Bytes, "UTF-8");
            }
            else if(rawValue instanceof GenericEnumSymbol || rawValue instanceof GenericData.EnumSymbol || rawValue instanceof GenericFixed || rawValue instanceof GenericRecord){
                throw new Exception("The Avro codec does not support this data type presently");
            }
            return rawValue;
        }
        catch (Exception e){
            return rawValue;
        }
    }

}