/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.plugins.fs.LocalInputFile;
import org.opensearch.dataprepper.plugins.fs.LocalOutputFile;
import org.opensearch.dataprepper.plugins.s3keyindex.S3ObjectIndexUtility;
import org.opensearch.dataprepper.plugins.sink.s3.S3OutputCodecContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

@DataPrepperPlugin(name = "parquet", pluginType = OutputCodec.class, pluginConfigurationType = ParquetOutputCodecConfig.class)
public class ParquetOutputCodec implements OutputCodec {
    private static final Logger LOG = LoggerFactory.getLogger(ParquetOutputCodec.class);

    private final ParquetOutputCodecConfig config;
    private static final String BASE_SCHEMA_STRING = "{\"type\":\"record\",\"name\":\"ParquetRecords\",\"fields\":[";
    private static final String END_SCHEMA_STRING = "]}";
    private static Schema schema;
    private ParquetWriter<GenericRecord> writer;
    private OutputCodecContext codecContext;
    private static final String PARQUET = "parquet";

    private static final String TIME_PATTERN_REGULAR_EXPRESSION = "\\%\\{.*?\\}";
    private static final Pattern SIMPLE_DURATION_PATTERN = Pattern.compile(TIME_PATTERN_REGULAR_EXPRESSION);
    private String key;
    private static final List<String> primitiveTypes = Arrays.asList("int", "long", "string", "float", "double", "bytes");


    @DataPrepperPluginConstructor
    public ParquetOutputCodec(final ParquetOutputCodecConfig config) {
        Objects.requireNonNull(config);
        this.config = config;
    }

    @Override
    public synchronized void start(final OutputStream outputStream, final Event event, final OutputCodecContext codecContext) throws IOException {
        Objects.requireNonNull(outputStream);
        Objects.requireNonNull(codecContext);
        if(!(outputStream instanceof PositionOutputStream)) {
            throw new RuntimeException("The Parquet output codec only works with the S3OutputStream and thus only with multi-part uploads.");
        }
        if(!(codecContext instanceof S3OutputCodecContext)) {
            throw new RuntimeException("The Parquet output codec only works with S3 presently");
        }
        S3OutputStream s3OutputStream = (S3OutputStream) outputStream;
        CompressionCodecName compressionCodecName = CompressionConverter.convertCodec(((S3OutputCodecContext) codecContext).getCompressionOption());
        this.codecContext = codecContext;
        buildSchemaAndKey(event, codecContext.getTagsTargetKey());
        final S3OutputFile s3OutputFile = new S3OutputFile(s3OutputStream);
        buildWriter(s3OutputFile, compressionCodecName);
    }

    @Override
    public boolean isCompressionInternal() {
        return true;
    }

    public synchronized void start(File file, final OutputCodecContext codecContext) throws IOException {
        Objects.requireNonNull(codecContext);
        this.codecContext = codecContext;
        LocalOutputFile localOutputFile = new LocalOutputFile(file);
        buildSchemaAndKey(null, null);
        buildWriter(localOutputFile, CompressionCodecName.UNCOMPRESSED);
    }

    void buildSchemaAndKey(final Event event, final String tagsTargetKey) throws IOException {
        if (config.getSchema() != null) {
            schema = parseSchema(config.getSchema());
        } else if (config.getFileLocation() != null) {
            schema = ParquetSchemaParser.parseSchemaFromJsonFile(config.getFileLocation());
        } else if (config.getSchemaRegistryUrl() != null) {
            schema = parseSchema(ParquetSchemaParserFromSchemaRegistry.getSchemaType(config.getSchemaRegistryUrl()));
        } else if (checkS3SchemaValidity()) {
            schema = ParquetSchemaParserFromS3.parseSchema(config);
        } else {
            schema = buildInlineSchemaFromEvent(event, tagsTargetKey);
        }
        key = generateKey();
    }

    public Schema buildInlineSchemaFromEvent(final Event event, final String tagsTargetKey) throws IOException {
        if (tagsTargetKey != null) {
            return parseSchema(buildSchemaStringFromEventMap(addTagsToEvent(event, tagsTargetKey).toMap(), false));
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
            if (codecContext.getExcludeKeys().contains(key)) {
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

    private void buildWriter(OutputFile outputFile, CompressionCodecName compressionCodecName) throws IOException {
        writer = AvroParquetWriter.<GenericRecord>builder(outputFile)
                .withSchema(schema)
                .withCompressionCodec(compressionCodecName)
                .build();
    }

    @Override
    public void writeEvent(final Event event, final OutputStream outputStream) throws IOException {
        final GenericData.Record parquetRecord = new GenericData.Record(schema);
        final Event modifiedEvent;
        if (codecContext.getTagsTargetKey() != null) {
            modifiedEvent = addTagsToEvent(event, codecContext.getTagsTargetKey());
        } else {
            modifiedEvent = event;
        }
        for (final String key : modifiedEvent.toMap().keySet()) {
            if (codecContext.getExcludeKeys().contains(key)) {
                continue;
            }
            final Schema.Field field = schema.getField(key);
            final Object value = schemaMapper(field, modifiedEvent.toMap().get(key));
            parquetRecord.put(key, value);
        }
        writer.write(parquetRecord);
    }

    @Override
    public synchronized void complete(final OutputStream outputStream) throws IOException {
        writer.close();
    }

    public void closeWriter(final OutputStream outputStream, File file) throws IOException {
        final LocalInputFile inputFile = new LocalInputFile(file);
        byte[] byteBuffer = inputFile.newStream().readAllBytes();
        outputStream.write(byteBuffer);
        writer.close();
    }

    @Override
    public String getExtension() {
        return PARQUET;
    }

    static Schema parseSchema(final String schemaString) {
        return new Schema.Parser().parse(schemaString);
    }

    /**
     * Generate the s3 object path prefix and object file name.
     *
     * @return object key path.
     */
    protected String generateKey() {
        final String pathPrefix = buildObjectPath(config.getPathPrefix());
        final String namePattern = buildObjectFileName(config.getNamePattern());
        return (!pathPrefix.isEmpty()) ? pathPrefix + namePattern : namePattern;
    }

    private static String buildObjectPath(final String pathPrefix) {
        final StringBuilder s3ObjectPath = new StringBuilder();
        if (pathPrefix != null && !pathPrefix.isEmpty()) {
            String[] pathPrefixList = pathPrefix.split("\\/");
            for (final String prefixPath : pathPrefixList) {
                if (SIMPLE_DURATION_PATTERN.matcher(prefixPath).find()) {
                    s3ObjectPath.append(S3ObjectIndexUtility.getObjectPathPrefix(prefixPath)).append("/");
                } else {
                    s3ObjectPath.append(prefixPath).append("/");
                }
            }
        }
        return s3ObjectPath.toString();
    }

    private String buildObjectFileName(final String configNamePattern) {
        return S3ObjectIndexUtility.getObjectNameWithDateTimeId(configNamePattern) + "." + getExtension();
    }

    private static Object schemaMapper(final Schema.Field field, final Object rawValue) {
        Object finalValue = null;
        final String fieldType = field.schema().getType().name().toLowerCase();
        if (field.schema().getLogicalType() == null && primitiveTypes.contains(fieldType)) {
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
            final String logicalTypeName = field.schema().getLogicalType().getName();
            switch (logicalTypeName) {
                case "date":
                    finalValue = Integer.parseInt(rawValue.toString());
                    break;
                case "time-millis":
                case "timestamp-millis":
                case "time-micros":
                case "timestamp-micros":
                    finalValue = Long.parseLong(rawValue.toString());
                    break;
                case "decimal":
                    Double.parseDouble(rawValue.toString());
                    break;
                case "uuid":
                    finalValue = rawValue.toString().getBytes(StandardCharsets.UTF_8);
                    break;
                default:
                    LOG.error("Unrecognised Logical Datatype for field : '{}' & type : '{}'", field.name(), logicalTypeName);
                    break;
            }
        }
        return finalValue;
    }

    boolean checkS3SchemaValidity() {
        if (config.getSchemaBucket() != null && config.getFileKey() != null && config.getSchemaRegion() != null) {
            return true;
        } else {
            return false;
        }
    }
}
