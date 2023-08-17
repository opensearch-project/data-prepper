/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.codec.avro;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.IOException;
import java.util.Map;

public class AvroSchemaParserFromS3 {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final ApacheHttpClient.Builder apacheHttpClientBuilder = ApacheHttpClient.builder();
    private static final Logger LOG = LoggerFactory.getLogger(AvroSchemaParserFromS3.class);

    public static Schema parseSchema(AvroOutputCodecConfig config) {
        try {
            return new Schema.Parser().parse(getS3SchemaObject(config));
        } catch (Exception e) {
            LOG.error("Unable to retrieve schema from S3.", e);
            throw new RuntimeException("There is an error in the schema: " + e.getMessage());
        }
    }

    private static String getS3SchemaObject(AvroOutputCodecConfig config) throws IOException {
        S3Client s3Client = buildS3Client(config);
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(config.getBucketName())
                .key(config.getFileKey())
                .build();
        ResponseInputStream<GetObjectResponse> s3Object = s3Client.getObject(getObjectRequest);
        final Map<String, Object> stringObjectMap = objectMapper.readValue(s3Object, new TypeReference<>() {
        });
        return objectMapper.writeValueAsString(stringObjectMap);
    }

    private static S3Client buildS3Client(AvroOutputCodecConfig config) {
        final AwsCredentialsProvider credentialsProvider = AwsCredentialsProviderChain.builder()
                .addCredentialsProvider(DefaultCredentialsProvider.create()).build();
        return S3Client.builder()
                .region(Region.of(config.getRegion()))
                .credentialsProvider(credentialsProvider)
                .httpClientBuilder(apacheHttpClientBuilder)
                .build();
    }
}