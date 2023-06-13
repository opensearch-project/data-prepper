/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import com.linecorp.armeria.client.retry.Backoff;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import java.time.Duration;
import java.util.Objects;

public class SqsService {
    private static final Logger LOG = LoggerFactory.getLogger(SqsService.class);
    static final long INITIAL_DELAY = Duration.ofSeconds(20).toMillis();
    static final long MAXIMUM_DELAY = Duration.ofMinutes(5).toMillis();
    static final double JITTER_RATE = 0.20;

    private final S3SourceConfig s3SourceConfig;
    private final S3Service s3Accessor;
    private final SqsClient sqsClient;
    private final PluginMetrics pluginMetrics;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final S3EventMessageParser s3EventMessageParser;

    private Thread sqsWorkerThread;

    public SqsService(final AcknowledgementSetManager acknowledgementSetManager,
                      final S3SourceConfig s3SourceConfig,
                      final S3Service s3Accessor,
                      final PluginMetrics pluginMetrics,
                      final AwsCredentialsProvider credentialsProvider) {
        this.s3SourceConfig = s3SourceConfig;
        this.s3Accessor = s3Accessor;
        this.pluginMetrics = pluginMetrics;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.sqsClient = createSqsClient(credentialsProvider);

        if (s3SourceConfig.getAcknowledgements() && Objects.isNull(s3SourceConfig.getSqsOptions().getVisibilityTimeout())) {
            setVisibilityTimeoutFromQueueAttribute();
        }

        s3EventMessageParser = new S3EventMessageParser();
    }

    public void start() {
        final Backoff backoff = Backoff.exponential(INITIAL_DELAY, MAXIMUM_DELAY).withJitter(JITTER_RATE)
                .withMaxAttempts(Integer.MAX_VALUE);
        sqsWorkerThread = new Thread(new SqsWorker(acknowledgementSetManager, sqsClient, s3Accessor, s3SourceConfig, pluginMetrics, s3EventMessageParser, backoff));
        sqsWorkerThread.start();
    }

    SqsClient createSqsClient(final AwsCredentialsProvider credentialsProvider) {
        LOG.info("Creating SQS client");
        return SqsClient.builder()
                .region(s3SourceConfig.getAwsAuthenticationOptions().getAwsRegion())
                .credentialsProvider(credentialsProvider)
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .retryPolicy(RetryPolicy.builder().numRetries(5).build())
                        .build())
                .build();
    }

    public void stop() {
        sqsWorkerThread.interrupt();
    }

    private void setVisibilityTimeoutFromQueueAttribute() {
        final GetQueueAttributesResponse getQueueAttributesResponse = sqsClient.getQueueAttributes(GetQueueAttributesRequest.builder()
                .queueUrl(s3SourceConfig.getSqsOptions().getSqsUrl())
                .attributeNames(QueueAttributeName.VISIBILITY_TIMEOUT)
                .build());

        if (getQueueAttributesResponse.attributes().containsKey(QueueAttributeName.VISIBILITY_TIMEOUT)) {
            s3SourceConfig.getSqsOptions().setVisibilityTimeout(Duration.ofSeconds(Long.parseLong(getQueueAttributesResponse.attributes().get(QueueAttributeName.VISIBILITY_TIMEOUT))));
        }
    }
}
