/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sts.StsClient;

public class SqsService {
    private static final Logger LOG = LoggerFactory.getLogger(SqsService.class);

    private final S3SourceConfig s3SourceConfig;
    private final S3Service s3Accessor;
    private final SqsClient sqsClient;

    private Thread sqsWorkerThread;

    public SqsService(final S3SourceConfig s3SourceConfig, final S3Service s3Accessor) {
        this.s3SourceConfig = s3SourceConfig;
        this.s3Accessor = s3Accessor;
        this.sqsClient = createSqsClient(StsClient.create());
    }

    public void start() {
        sqsWorkerThread = new Thread(new SqsWorker(sqsClient, s3Accessor, s3SourceConfig));
        sqsWorkerThread.start();
    }

    SqsClient createSqsClient(final StsClient stsClient) {
        LOG.info("Creating SQS client");
        return SqsClient.builder()
                .region(Region.of(s3SourceConfig.getAWSAuthenticationOptions().getAwsRegion()))
                .credentialsProvider(s3SourceConfig.getAWSAuthenticationOptions().authenticateAwsConfiguration(stsClient))
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .retryPolicy(RetryPolicy.builder().numRetries(5).build())
                        .build())
                .build();
    }

    public void stop() {
        sqsWorkerThread.interrupt();
    }
}
