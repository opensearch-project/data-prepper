/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.s3_enricher.processor.client;

import org.opensearch.dataprepper.plugins.s3_enricher.processor.S3EnricherProcessorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * A Builder Factory for creating and fetching the <code>S3Client</code> and <code>S3AsyncClient</code>.
 */
public class S3ClientBuilderFactory {
    private static final Logger LOG = LoggerFactory.getLogger(S3ClientBuilderFactory.class);
    private final S3EnricherProcessorConfig s3SourceConfig;
    private final AwsCredentialsProvider credentialsProvider;
    private final S3Client s3Client;
    public S3ClientBuilderFactory(final S3EnricherProcessorConfig s3SourceConfig, AwsCredentialsProvider credentialsProvider){
        this.s3SourceConfig = s3SourceConfig;
        this.credentialsProvider = credentialsProvider;
        this.s3Client = createS3Client();
    }
    /**
     * Create a S3Client Object for download the s3 Objects
     * @return a S3Client Object
     */
    public S3Client createS3Client() {
        LOG.info("Creating S3 client");
            return S3Client.builder()
                .region(s3SourceConfig.getAwsAuthenticationOptions().getAwsRegion())
                .crossRegionAccessEnabled(true)
                .credentialsProvider(credentialsProvider)
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
}