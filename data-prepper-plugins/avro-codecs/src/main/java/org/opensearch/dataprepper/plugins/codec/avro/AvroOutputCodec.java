/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.opensearch.dataprepper.avro.AvroAutoSchemaGenerator;
import org.opensearch.dataprepper.avro.AvroEventConverter;
import org.opensearch.dataprepper.avro.EventDefinedAvroEventConverter;
import org.opensearch.dataprepper.avro.SchemaDefinedAvroEventConverter;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Objects;

/**
 * An implementation of {@link OutputCodec} which deserializes Data-Prepper events
 * and writes them to Output Stream as AVRO Data
 */
@DataPrepperPlugin(name = "avro", pluginType = OutputCodec.class, pluginConfigurationType = AvroOutputCodecConfig.class)
public class AvroOutputCodec implements OutputCodec {
    private static final Logger LOG = LoggerFactory.getLogger(AvroOutputCodec.class);
    private static final String AVRO = "avro";

    private final AvroOutputCodecConfig config;
    private final AvroEventConverter avroEventConverter;
    private final AvroAutoSchemaGenerator avroAutoSchemaGenerator;
    private DataFileWriter<GenericRecord> dataFileWriter;

    private Schema schema;

    private OutputCodecContext codecContext;

    @DataPrepperPluginConstructor
    public AvroOutputCodec(final AvroOutputCodecConfig config) {
        Objects.requireNonNull(config);
        this.config = config;

        avroAutoSchemaGenerator = new AvroAutoSchemaGenerator();

        if (config.getSchema() != null) {
            schema = parseSchema(config.getSchema());
            avroEventConverter = new SchemaDefinedAvroEventConverter();
        } else {
            avroEventConverter = new EventDefinedAvroEventConverter();
        }
    }

    @Override
    public void start(final OutputStream outputStream, final Event event, final OutputCodecContext codecContext) throws IOException {
        Objects.requireNonNull(outputStream);
        Objects.requireNonNull(codecContext);
        this.codecContext = codecContext;
        if (schema == null) {
            schema = buildInlineSchemaFromEvent(event);
        }
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(schema, outputStream);
    }

    public Schema buildInlineSchemaFromEvent(final Event event) throws IOException {
        final Map<String, Object> data;
        if (codecContext != null && codecContext.getTagsTargetKey() != null) {
            data = addTagsToEvent(event, codecContext.getTagsTargetKey()).toMap();
        } else {
            data = event.toMap();
        }

        return avroAutoSchemaGenerator.autoDetermineSchema(data, codecContext);
    }

    @Override
    public void complete(final OutputStream outputStream) throws IOException {
        dataFileWriter.close();
        outputStream.close();
    }

    @Override
    public void writeEvent(final Event event, final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(event);
        final Map<String, Object> data;
        if (codecContext.getTagsTargetKey() != null) {
            data = addTagsToEvent(event, codecContext.getTagsTargetKey()).toMap();
        } else {
            data = event.toMap();
        }
        final GenericRecord avroRecord = avroEventConverter.convertEventDataToAvro(schema, data, codecContext);
        dataFileWriter.append(avroRecord);
    }

    @Override
    public String getExtension() {
        return AVRO;
    }

    @Override
    public void validateAgainstCodecContext(OutputCodecContext outputCodecContext) {
        if (config.isAutoSchema())
            return;

        if ((outputCodecContext.getIncludeKeys() != null && !outputCodecContext.getIncludeKeys().isEmpty()) ||
                (outputCodecContext.getExcludeKeys() != null && !outputCodecContext.getExcludeKeys().isEmpty())) {
            throw new InvalidPluginConfigurationException("Providing a user-defined schema and using sink include or exclude keys is not an allowed configuration.");
        }
    }

    Schema parseSchema(final String schemaString) {
        try {
            Objects.requireNonNull(schemaString);
            return new Schema.Parser().parse(schemaString);
        } catch (Exception e) {
            LOG.error("Unable to parse Schema from Schema String provided.", e);
            throw new RuntimeException("There is an error in the schema: " + e.getMessage());
        }
    }
}