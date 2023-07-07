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
import org.apache.parquet.io.OutputFile;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.fs.LocalInputFile;
import org.opensearch.dataprepper.plugins.fs.LocalOutputFile;
import org.opensearch.dataprepper.plugins.s3keyindex.S3ObjectIndexUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

@DataPrepperPlugin(name = "parquet", pluginType = OutputCodec.class, pluginConfigurationType = ParquetOutputCodecConfig.class)
public class ParquetOutputCodec implements OutputCodec {
    private static final Logger LOG =  LoggerFactory.getLogger(ParquetOutputCodec.class);

    private final ParquetOutputCodecConfig config;
    private static Schema schema;
    private ParquetWriter<GenericRecord> writer;
    private final ApacheHttpClient.Builder apacheHttpClientBuilder = ApacheHttpClient.builder();
    private S3Client s3Client;
    private static final String PARQUET = "parquet";

    private static final String TIME_PATTERN_REGULAR_EXPRESSION = "\\%\\{.*?\\}";
    private static final Pattern SIMPLE_DURATION_PATTERN = Pattern.compile(TIME_PATTERN_REGULAR_EXPRESSION);
    private String key;
    private final String bucket;
    private final String region;
    private static final List<String> primitiveTypes = Arrays.asList("int", "long", "string", "float", "double", "bytes");


    @DataPrepperPluginConstructor
    public ParquetOutputCodec(final ParquetOutputCodecConfig config) {
        Objects.requireNonNull(config);
        this.config = config;
        this.region = config.getRegion();
        this.bucket = config.getBucket();
    }

    @Override
    public synchronized void start(final OutputStream outputStream) throws IOException {
        this.s3Client = buildS3Client();
        buildSchemaAndKey();
        final S3OutputFile s3OutputFile = new S3OutputFile(s3Client, bucket, key);
        buildWriter(s3OutputFile);
    }

    public synchronized void start(File file) throws IOException {
        LocalOutputFile localOutputFile =new LocalOutputFile(file);
        buildSchemaAndKey();
        buildWriter(localOutputFile);
    }

    private void buildSchemaAndKey() throws IOException {
        if (config.getSchema() != null) {
            schema = parseSchema(config.getSchema());
        } else if(config.getFileLocation()!=null){
            schema = ParquetSchemaParser.parseSchemaFromJsonFile(config.getFileLocation());
        }else if(config.getSchemaRegistryUrl()!=null){
            schema = parseSchema(ParquetSchemaParserFromSchemaRegistry.getSchemaType(config.getSchemaRegistryUrl()));
        }else if(checkS3SchemaValidity()){
            schema = ParquetSchemaParserFromS3.parseSchema(config);
        }
        else{
            LOG.error("Schema not provided.");
            throw new IOException("Can't proceed without Schema.");
        }
        key = generateKey();
    }

    private void buildWriter(OutputFile outputFile) throws IOException {
        writer = AvroParquetWriter.<GenericRecord>builder(outputFile)
                .withSchema(schema)
                .build();
    }

    @Override
    public void writeEvent(final Event event, final OutputStream outputStream,final String tagsTargetKey) throws IOException {
        final GenericData.Record parquetRecord = new GenericData.Record(schema);
        final Event modifiedEvent;
        if (tagsTargetKey != null) {
            modifiedEvent = addTagsToEvent(event, tagsTargetKey);
        } else {
            modifiedEvent = event;
        }
        for (final String key : modifiedEvent.toMap().keySet()) {
            if (config.getExcludeKeys().contains(key)) {
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
        final S3ObjectReference s3ObjectReference = S3ObjectReference.bucketAndKey(bucket, key).build();
        final S3InputFile inputFile = new S3InputFile(s3Client, s3ObjectReference);
        byte[] byteBuffer = inputFile.newStream().readAllBytes();
        outputStream.write(byteBuffer);
        final DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
        s3Client.deleteObject(deleteRequest);
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

    private S3Client buildS3Client() {
        final AwsCredentialsProvider credentialsProvider = AwsCredentialsProviderChain.builder()
                .addCredentialsProvider(DefaultCredentialsProvider.create()).build();
        return S3Client.builder()
                .region(Region.of(region))
                .credentialsProvider(credentialsProvider)
                .httpClientBuilder(apacheHttpClientBuilder)
                .build();
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
    private static Object schemaMapper(final Schema.Field field , final Object rawValue){
        Object finalValue = null;
        final String fieldType = field.schema().getType().name().toString().toLowerCase();
        if (field.schema().getLogicalType() == null && primitiveTypes.contains(fieldType)){
            switch (fieldType){
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
        }else{
            final String logicalTypeName = field.schema().getLogicalType().getName();
            switch (logicalTypeName){
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
        return  finalValue;
    }
    boolean checkS3SchemaValidity() throws IOException {
        if (config.getSchemaBucket() != null && config.getFileKey() != null && config.getSchemaRegion() != null) {
            return true;
        } else {
            LOG.error("Invalid S3 credentials, can't reach the schema file.");
            throw new IOException("Can't proceed without schema.");
        }
    }
}
