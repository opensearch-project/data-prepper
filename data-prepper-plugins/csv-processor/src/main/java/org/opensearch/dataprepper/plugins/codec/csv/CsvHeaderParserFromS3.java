/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.codec.csv;

import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CsvHeaderParserFromS3 {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final ApacheHttpClient.Builder apacheHttpClientBuilder = ApacheHttpClient.builder();
    private static final Logger LOG = LoggerFactory.getLogger(CsvHeaderParserFromS3.class);

    public static List<String> parseHeader(CsvOutputCodecConfig config) throws IOException {
        try{
            BufferedReader reader = new BufferedReader(new InputStreamReader(getS3SchemaObject(config)));
            final List<String> headerList = new ArrayList<>();
            final String[] header = reader.readLine().split(",");
            if (header != null) {
                Collections.addAll(headerList, header);
            } else {
                throw new IOException("Header not found in CSV Header file.");
            }
            return headerList;
        }
        catch(Exception e){
            LOG.error("Unable to retrieve header from S3. Error: "+e.getMessage());
            throw new IOException("Can't proceed without header.");
        }
    }

    private static InputStream getS3SchemaObject(CsvOutputCodecConfig config) throws IOException {
        S3Client s3Client = buildS3Client(config);
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(config.getBucketName())
                .key(config.getFile_key())
                .build();
        ResponseInputStream<GetObjectResponse> s3Object = s3Client.getObject(getObjectRequest);
        return s3Object;
    }

    private static S3Client buildS3Client(CsvOutputCodecConfig config) {
        final AwsCredentialsProvider credentialsProvider = AwsCredentialsProviderChain.builder()
                .addCredentialsProvider(DefaultCredentialsProvider.create()).build();
        return S3Client.builder()
                .region(Region.of(config.getRegion()))
                .credentialsProvider(credentialsProvider)
                .httpClientBuilder(apacheHttpClientBuilder)
                .build();
    }
}
