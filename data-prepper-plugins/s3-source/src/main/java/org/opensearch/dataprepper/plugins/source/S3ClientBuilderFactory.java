/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Duration;

/**
 * A Builder Factory for creating and fetching the <code>S3Client</code> and <code>S3AsyncClient</code>.
 */
public class S3ClientBuilderFactory {
    private static final Logger LOG = LoggerFactory.getLogger(S3ClientBuilderFactory.class);
    private final S3SourceConfig s3SourceConfig;
    private final S3Client s3Client;
    private final S3AsyncClient s3AsyncClient;
    public S3ClientBuilderFactory(final S3SourceConfig s3SourceConfig){
        this.s3SourceConfig = s3SourceConfig;
        this.s3Client = createS3Client();
        this.s3AsyncClient = createS3AsyncClient();
    }
    /**
     * Create a S3Client Object for download the s3 Objects
     * @return a S3Client Object
     */
    public S3Client createS3Client() {
        LOG.info("Creating S3 client");
            return S3Client.builder()
                .region(s3SourceConfig.getAwsAuthenticationOptions().getAwsRegion())
                .credentialsProvider(s3SourceConfig.getAwsAuthenticationOptions().authenticateAwsConfiguration())
                    .overrideConfiguration(ClientOverrideConfiguration.builder()
                            .retryPolicy(retryPolicy -> retryPolicy.numRetries(5).build())
                            .build())
                    .build();
    }

    /**
     * Create a S3AsyncClient Object for S3 Select query
     * @return a S3AsyncClient Object
     */
    public S3AsyncClient createS3AsyncClient() {
        LOG.info("Creating S3 Async client");
        return S3AsyncClient.builder()
                .region(s3SourceConfig.getAwsAuthenticationOptions().getAwsRegion())
                .httpClient(NettyNioAsyncHttpClient.builder()
                        .maxConcurrency(200)
                        .connectionTimeout(Duration.ofMinutes(1)).build())
                .credentialsProvider(s3SourceConfig.getAwsAuthenticationOptions().authenticateAwsConfiguration())
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .retryPolicy(retryPolicy -> retryPolicy.numRetries(5).build())
                        .build())
                .build();
    }

    /**
     * get the S3Client Object
     * @return a S3AClient Object
     */
    public S3Client getS3Client() {
        return s3Client;
    }

    /**
     * get the S3AsyncClient Object
     * @return a S3AClient Object
     */
    public S3AsyncClient getS3AsyncClient() {
        return s3AsyncClient;
    }
}