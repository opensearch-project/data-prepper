/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.avro;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * An implementation of {@link OutputCodec} which deserializes Data-Prepper events
 * and writes them to Output Stream as AVRO Data
 */
@DataPrepperPlugin(name = "avro", pluginType = OutputCodec.class, pluginConfigurationType = AvroOutputCodecConfig.class)
public class AvroOutputCodec implements OutputCodec {

    private static final List<String> nonComplexTypes = Arrays.asList("int", "long", "string", "float", "double", "bytes");
    private static final Logger LOG = LoggerFactory.getLogger(AvroOutputCodec.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final String AVRO = "avro";
    private final AvroOutputCodecConfig config;
    private DataFileWriter<GenericRecord> dataFileWriter;

    private Schema schema;

    @DataPrepperPluginConstructor
    public AvroOutputCodec(final AvroOutputCodecConfig config) {
        Objects.requireNonNull(config);
        this.config = config;
    }

    @Override
    public void start(final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(outputStream);
        if (config.getSchema() != null) {
            schema = parseSchema(config.getSchema());
        } else if (config.getFileLocation() != null) {
            schema = AvroSchemaParser.parseSchemaFromJsonFile(config.getFileLocation());
        } else if (config.getSchemaRegistryUrl() != null) {
            schema = parseSchema(AvroSchemaParserFromSchemaRegistry.getSchemaType(config.getSchemaRegistryUrl()));
        } else if (checkS3SchemaValidity()) {
            schema = AvroSchemaParserFromS3.parseSchema(config);
        } else {
            LOG.error("Schema not provided.");
            throw new IOException("Can't proceed without Schema.");
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
    public void writeEvent(final Event event, final OutputStream outputStream, final String tagsTargetKey) throws IOException {
        Objects.requireNonNull(event);
        if (tagsTargetKey != null) {
            final GenericRecord avroRecord = buildAvroRecord(schema, addTagsToEvent(event, tagsTargetKey).toMap());
            dataFileWriter.append(avroRecord);
        } else {
            final GenericRecord avroRecord = buildAvroRecord(schema, event.toMap());
            dataFileWriter.append(avroRecord);
        }
    }

    @Override
    public String getExtension() {
        return AVRO;
    }

    Schema parseSchema(final String schemaString) throws IOException {
        try {
            Objects.requireNonNull(schemaString);
            return new Schema.Parser().parse(schemaString);
        } catch (Exception e) {
            LOG.error("Unable to parse Schema from Schema String provided.");
            throw new IOException("Can't proceed without schema.");
        }
    }

    private GenericRecord buildAvroRecord(final Schema schema, final Map<String, Object> eventData) {
        final GenericRecord avroRecord = new GenericData.Record(schema);
        final boolean isExcludeKeyAvailable = !Objects.isNull(config.getExcludeKeys());
        for (final String key : eventData.keySet()) {
            if (isExcludeKeyAvailable && config.getExcludeKeys().contains(key)) {
                continue;
            }
            final Schema.Field field = schema.getField(key);
            final Object value = schemaMapper(field, eventData.get(key));
            avroRecord.put(key, value);
        }
        return avroRecord;
    }

    private Object schemaMapper(final Schema.Field field, final Object rawValue) {
        Object finalValue = null;
        final String fieldType = field.schema().getType().name().toLowerCase();
        if (nonComplexTypes.contains(fieldType)) {
            switch (fieldType) {
                case "string":
                    finalValue = rawValue.toString();
                    break;
                case "int":
                    finalValue = Integer.parseInt(rawValue.toString());
                    break;
                case "float":
                    finalValue = Float.parseFloat(rawValue.toString());
                    break;
                case "double":
                    finalValue = Double.parseDouble(rawValue.toString());
                    break;
                case "long":
                    finalValue = Long.parseLong(rawValue.toString());
                    break;
                case "bytes":
                    finalValue = rawValue.toString().getBytes(StandardCharsets.UTF_8);
                    break;
                default:
                    LOG.error("Unrecognised Field name : '{}' & type : '{}'", field.name(), fieldType);
                    break;
            }
        } else {
            if (fieldType.equals("record") && rawValue instanceof Map) {
                finalValue = buildAvroRecord(field.schema(), (Map<String, Object>) rawValue);
            } else if (fieldType.equals("array") && rawValue instanceof List) {
                GenericData.Array<String> avroArray =
                        new GenericData.Array<>(((List<String>) rawValue).size(), field.schema());
                for (String element : ((List<String>) rawValue)) {
                    avroArray.add(element);
                }
                finalValue = avroArray;
            }
        }
        return finalValue;
    }

    private boolean checkS3SchemaValidity() throws IOException {
        if (config.getBucketName() != null && config.getFileKey() != null && config.getRegion() != null) {
            return true;
        } else {
            LOG.error("Invalid S3 credentials, can't reach the schema file.");
            throw new IOException("Can't proceed without schema.");
        }
    }
}