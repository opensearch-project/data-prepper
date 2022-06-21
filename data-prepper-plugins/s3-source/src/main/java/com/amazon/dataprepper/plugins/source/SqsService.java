/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.metrics.PluginMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

public class SqsService {
    private static final Logger LOG = LoggerFactory.getLogger(SqsService.class);

    private final S3SourceConfig s3SourceConfig;
    private final S3Service s3Accessor;
    private final SqsClient sqsClient;
    private final PluginMetrics pluginMetrics;

    private Thread sqsWorkerThread;

    public SqsService(final S3SourceConfig s3SourceConfig,
                      final S3Service s3Accessor,
                      final PluginMetrics pluginMetrics) {
        this.s3SourceConfig = s3SourceConfig;
        this.s3Accessor = s3Accessor;
        this.pluginMetrics = pluginMetrics;
        this.sqsClient = createSqsClient();
    }

    public void start() {
        sqsWorkerThread = new Thread(new SqsWorker(sqsClient, s3Accessor, s3SourceConfig, pluginMetrics));
        sqsWorkerThread.start();
    }

    SqsClient createSqsClient() {
        LOG.info("Creating SQS client");
        return SqsClient.builder()
                .region(Region.of(s3SourceConfig.getAwsAuthenticationOptions().getAwsRegion()))
                .credentialsProvider(s3SourceConfig.getAwsAuthenticationOptions().authenticateAwsConfiguration())
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .retryPolicy(RetryPolicy.builder().numRetries(5).build())
                        .build())
                .build();
    }

    public void stop() {
        sqsWorkerThread.interrupt();
    }
}
