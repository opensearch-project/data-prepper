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
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
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

    private static final List<String> NON_COMPLEX_TYPES = Arrays.asList("int", "long", "string", "float", "double", "bytes");
    private static final Logger LOG = LoggerFactory.getLogger(AvroOutputCodec.class);
    private static final String AVRO = "avro";
    private static final String BASE_SCHEMA_STRING = "{\"type\":\"record\",\"name\":\"AvroRecords\",\"fields\":[";
    private static final String END_SCHEMA_STRING = "]}";

    private final AvroOutputCodecConfig config;
    private DataFileWriter<GenericRecord> dataFileWriter;

    private Schema schema;

    private OutputCodecContext codecContext;

    @DataPrepperPluginConstructor
    public AvroOutputCodec(final AvroOutputCodecConfig config) {
        Objects.requireNonNull(config);
        this.config = config;

        if (config.getSchema() != null) {
            schema = parseSchema(config.getSchema());
        } else if (config.getFileLocation() != null) {
            schema = AvroSchemaParser.parseSchemaFromJsonFile(config.getFileLocation());
        } else if (config.getSchemaRegistryUrl() != null) {
            schema = parseSchema(AvroSchemaParserFromSchemaRegistry.getSchemaType(config.getSchemaRegistryUrl()));
        } else if (checkS3SchemaValidity()) {
            schema = AvroSchemaParserFromS3.parseSchema(config);
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
        if (codecContext != null && codecContext.getTagsTargetKey() != null) {
            return parseSchema(buildSchemaStringFromEventMap(addTagsToEvent(event, codecContext.getTagsTargetKey()).toMap(), false));
        } else {
            return parseSchema(buildSchemaStringFromEventMap(event.toMap(), false));
        }
    }

    private String buildSchemaStringFromEventMap(final Map<String, Object> eventData, boolean nestedRecordFlag) {
        final StringBuilder builder = new StringBuilder();
        int nestedRecordIndex = 1;
        if (!nestedRecordFlag) {
            builder.append(BASE_SCHEMA_STRING);
        } else {
            builder.append("{\"type\":\"record\",\"name\":\"" + "NestedRecord" + nestedRecordIndex + "\",\"fields\":[");
            nestedRecordIndex++;
        }
        String fields;
        int index = 0;
        for (final String key : eventData.keySet()) {
            if (codecContext != null && codecContext.getExcludeKeys().contains(key)) {
                continue;
            }
            if (index == 0) {
                if (!(eventData.get(key) instanceof Map)) {
                    fields = "{\"name\":\"" + key + "\",\"type\":\"" + typeMapper(eventData.get(key)) + "\"}";
                } else {
                    fields = "{\"name\":\"" + key + "\",\"type\":" + typeMapper(eventData.get(key)) + "}";
                }
            } else {
                if (!(eventData.get(key) instanceof Map)) {
                    fields = "," + "{\"name\":\"" + key + "\",\"type\":\"" + typeMapper(eventData.get(key)) + "\"}";
                } else {
                    fields = "," + "{\"name\":\"" + key + "\",\"type\":" + typeMapper(eventData.get(key)) + "}";
                }
            }
            builder.append(fields);
            index++;
        }
        builder.append(END_SCHEMA_STRING);
        return builder.toString();
    }

    private String typeMapper(final Object value) {
        if (value instanceof Integer || value.getClass().equals(int.class)) {
            return "int";
        } else if (value instanceof Float || value.getClass().equals(float.class)) {
            return "float";
        } else if (value instanceof Double || value.getClass().equals(double.class)) {
            return "double";
        } else if (value instanceof Long || value.getClass().equals(long.class)) {
            return "long";
        } else if (value instanceof Byte[]) {
            return "bytes";
        } else if (value instanceof Map) {
            return buildSchemaStringFromEventMap((Map<String, Object>) value, true);
        } else {
            return "string";
        }
    }

    @Override
    public void complete(final OutputStream outputStream) throws IOException {
        dataFileWriter.close();
        outputStream.close();
    }

    @Override
    public void writeEvent(final Event event, final OutputStream outputStream) throws IOException {
        Objects.requireNonNull(event);
        if (codecContext.getTagsTargetKey() != null) {
            final GenericRecord avroRecord = buildAvroRecord(schema, addTagsToEvent(event, codecContext.getTagsTargetKey()).toMap());
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

    Schema parseSchema(final String schemaString) {
        try {
            Objects.requireNonNull(schemaString);
            return new Schema.Parser().parse(schemaString);
        } catch (Exception e) {
            LOG.error("Unable to parse Schema from Schema String provided.", e);
            throw new RuntimeException("There is an error in the schema: " + e.getMessage());
        }
    }

    private GenericRecord buildAvroRecord(final Schema schema, final Map<String, Object> eventData) {
        final GenericRecord avroRecord = new GenericData.Record(schema);
        final boolean isExcludeKeyAvailable = !codecContext.getExcludeKeys().isEmpty();
        for (final String key : eventData.keySet()) {
            if (isExcludeKeyAvailable && codecContext.getExcludeKeys().contains(key)) {
                continue;
            }
            final Schema.Field field = schema.getField(key);
            if (field == null) {
                throw new RuntimeException("The event has a key ('" + key + "') which is not included in the schema.");
            }
            final Object value = schemaMapper(field, eventData.get(key));
            avroRecord.put(key, value);
        }
        return avroRecord;
    }

    private Object schemaMapper(final Schema.Field field, final Object rawValue) {
        Object finalValue = null;
        final String fieldType = field.schema().getType().name().toLowerCase();
        if (NON_COMPLEX_TYPES.contains(fieldType)) {
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

    private boolean checkS3SchemaValidity() {
        return config.getBucketName() != null && config.getFileKey() != null && config.getRegion() != null;
    }
}