/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.IOException;

public class S3Service {
    private static final Logger LOG = LoggerFactory.getLogger(S3Service.class);

    private final S3SourceConfig s3SourceConfig;
    private final S3ObjectHandler s3ObjectHandler;

    S3Service(final S3ObjectHandler s3ObjectHandler,
    		  final S3SourceConfig s3SourceConfig) {
        this.s3SourceConfig = s3SourceConfig;
        this.s3ObjectHandler =s3ObjectHandler;
    }

    void addS3Object(final S3ObjectReference s3ObjectReference) {
//    	TODO: if conditional check not implemented here. 
//    	then we need to put some extra efforts to incorporate the changes 
		try {
			if (s3ObjectHandler instanceof S3ObjectWorker) {
				s3ObjectHandler.parseS3Object(s3ObjectReference,createS3Client());
			} else {
				s3ObjectHandler.parseS3Object(s3ObjectReference,createS3AsyncClient());
			}
		} catch (final IOException e) {
            LOG.error("Unable to read S3 object from S3ObjectReference = {}", s3ObjectReference, e);
        }
    }

    S3Client createS3Client() {
        LOG.info("Creating S3 client");
        return S3Client.builder()
                .region(s3SourceConfig.getAwsAuthenticationOptions().getAwsRegion())
                .credentialsProvider(s3SourceConfig.getAwsAuthenticationOptions().authenticateAwsConfiguration())
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .retryPolicy(RetryPolicy.builder().numRetries(5).build())
                        .build())
                .build();
    }

    /**
     * Create a S3AsyncClient Object for S3 Select query
     * @return a S3AsyncClient Object
     */
    S3AsyncClient createS3AsyncClient() {
        LOG.info("Creating S3 Async client");
        return S3AsyncClient.builder()
                .region(s3SourceConfig.getAwsAuthenticationOptions().getAwsRegion())
                .credentialsProvider(s3SourceConfig.getAwsAuthenticationOptions().authenticateAwsConfiguration())
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .retryPolicy(RetryPolicy.builder().numRetries(5).build())
                        .build())
                .build();
    }

}
