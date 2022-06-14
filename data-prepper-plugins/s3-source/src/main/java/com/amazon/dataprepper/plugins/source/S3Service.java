/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.plugins.source.codec.Codec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;

public class S3Service {
    private static final Logger LOG = LoggerFactory.getLogger(S3Service.class);

    private final S3SourceConfig s3SourceConfig;
    private final Buffer<Record<Event>> buffer;
    private final S3Client s3Client;
    private final Codec codec;

    public S3Service(final S3SourceConfig s3SourceConfig,  Buffer<Record<Event>> buffer, Codec codec) {
        this.s3SourceConfig = s3SourceConfig;
        this.buffer = buffer;
        this.codec = codec;
        this.s3Client = createS3Client(StsClient.create());
    }

    S3ObjectReference addS3Object(final S3ObjectReference s3ObjectReference) {
        // TODO: should return message id and receipt handle if successfully converted to event
        return null;
    }

    S3Client createS3Client(final StsClient stsClient) {
        LOG.info("Creating S3 client");
        return S3Client.builder()
                .region(Region.of(s3SourceConfig.getAWSAuthenticationOptions().getAwsRegion()))
                .credentialsProvider(s3SourceConfig.getAWSAuthenticationOptions().authenticateAwsConfiguration(stsClient))
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .retryPolicy(RetryPolicy.builder().numRetries(5).build())
                        .build())
                .build();
    }
}
