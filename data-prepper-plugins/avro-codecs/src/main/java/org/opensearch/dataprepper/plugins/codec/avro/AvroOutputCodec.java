/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;
/**
 * An implementation of {@link OutputCodec} which deserializes Data-Prepper events
 * and writes them to Output Stream as AVRO Data
 */
@DataPrepperPlugin(name = "avro", pluginType = OutputCodec.class, pluginConfigurationType = AvroOutputCodecConfig.class)
public class AvroOutputCodec implements OutputCodec {

    private final AvroOutputCodecConfig config;
    private static final Logger LOG = LoggerFactory.getLogger(AvroOutputCodec.class);

    @DataPrepperPluginConstructor
    public AvroOutputCodec(final AvroOutputCodecConfig config) {
        Objects.requireNonNull(config);
        this.config = config;
    }


    private DataFileWriter<GenericRecord> dataFileWriter;

    private Schema schema;

    private static final String AVRO = "avro";


    @Override
    public void start(final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(outputStream);
        if(config.getSchema()!=null){
            schema=parseSchema(config.getSchema());
        }
        else if(config.getFileLocation()!=null){
            schema = AvroSchemaParser.parseSchemaFromJsonFile(config.getFileLocation());
        }else{
            LOG.error("Schema not provided.");
        }
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(schema, outputStream);
    }

    @Override
    public void complete(final OutputStream outputStream) throws IOException {
        dataFileWriter.close();
        outputStream.close();
    }

    @Override
    public void writeEvent(final Event event,final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(event);
        final GenericRecord avroRecord = new GenericData.Record(schema);
        final boolean isExcludeKeyAvailable = !Objects.isNull(config.getExcludeKeys());
        for (final String key : event.toMap().keySet()) {
            if (isExcludeKeyAvailable && config.getExcludeKeys().contains(key)) {
                continue;
            }
            avroRecord.put(key, event.toMap().get(key));
        }
        dataFileWriter.append(avroRecord);
    }

    @Override
    public String getExtension() {
        return AVRO;
    }

     Schema parseSchema(final String schemaString) throws IOException {
        try{
            Objects.requireNonNull(schemaString);
            return new Schema.Parser().parse(schemaString);
        }catch(Exception e){
            LOG.error("Unable to parse Schema from Schema String provided.");
            throw new IOException("Can't proceed without schema.");
         }
     }
}


