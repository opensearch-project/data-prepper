/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.opensearch.dataprepper.avro.AvroAutoSchemaGenerator;
import org.opensearch.dataprepper.avro.AvroEventConverter;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.plugins.fs.LocalInputFile;
import org.opensearch.dataprepper.plugins.s3keyindex.S3ObjectIndexUtility;
import org.opensearch.dataprepper.plugins.sink.s3.S3OutputCodecContext;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

@DataPrepperPlugin(name = "parquet", pluginType = OutputCodec.class, pluginConfigurationType = ParquetOutputCodecConfig.class)
public class ParquetOutputCodec implements OutputCodec {
    private final ParquetOutputCodecConfig config;
    private static Schema schema;
    private final AvroEventConverter avroEventConverter;
    private final AvroAutoSchemaGenerator avroAutoSchemaGenerator;
    private ParquetWriter<GenericRecord> writer;
    private OutputCodecContext codecContext;
    private static final String PARQUET = "parquet";

    private static final String TIME_PATTERN_REGULAR_EXPRESSION = "\\%\\{.*?\\}";
    private static final Pattern SIMPLE_DURATION_PATTERN = Pattern.compile(TIME_PATTERN_REGULAR_EXPRESSION);
    private String key;


    @DataPrepperPluginConstructor
    public ParquetOutputCodec(final ParquetOutputCodecConfig config) {
        Objects.requireNonNull(config);
        this.config = config;

        avroEventConverter = new AvroEventConverter();
        avroAutoSchemaGenerator = new AvroAutoSchemaGenerator();
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
        PositionOutputStream s3OutputStream = (PositionOutputStream) outputStream;
        CompressionCodecName compressionCodecName = CompressionConverter.convertCodec(((S3OutputCodecContext) codecContext).getCompressionOption());
        this.codecContext = codecContext;
        buildSchemaAndKey(event);
        final S3OutputFile s3OutputFile = new S3OutputFile(s3OutputStream);
        buildWriter(s3OutputFile, compressionCodecName);
    }

    @Override
    public boolean isCompressionInternal() {
        return true;
    }

    void buildSchemaAndKey(final Event event) throws IOException {
        if (config.getSchema() != null) {
            schema = parseSchema(config.getSchema());
        } else if (config.getFileLocation() != null) {
            schema = ParquetSchemaParser.parseSchemaFromJsonFile(config.getFileLocation());
        } else if (config.getSchemaRegistryUrl() != null) {
            schema = parseSchema(ParquetSchemaParserFromSchemaRegistry.getSchemaType(config.getSchemaRegistryUrl()));
        } else if (checkS3SchemaValidity()) {
            schema = ParquetSchemaParserFromS3.parseSchema(config);
        } else {
            schema = buildInlineSchemaFromEvent(event);
        }
        key = generateKey();
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

    private void buildWriter(OutputFile outputFile, CompressionCodecName compressionCodecName) throws IOException {
        writer = AvroParquetWriter.<GenericRecord>builder(outputFile)
                .withSchema(schema)
                .withCompressionCodec(compressionCodecName)
                .build();
    }

    @Override
    public void writeEvent(final Event event, final OutputStream outputStream) throws IOException {
        final Event modifiedEvent;
        if (codecContext.getTagsTargetKey() != null) {
            modifiedEvent = addTagsToEvent(event, codecContext.getTagsTargetKey());
        } else {
            modifiedEvent = event;
        }
        GenericRecord parquetRecord = avroEventConverter.convertEventDataToAvro(schema, modifiedEvent.toMap(), codecContext);
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

    boolean checkS3SchemaValidity() {
        if (config.getSchemaBucket() != null && config.getFileKey() != null && config.getSchemaRegion() != null) {
            return true;
        } else {
            return false;
        }
    }
}
