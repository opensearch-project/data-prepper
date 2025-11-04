/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.sqs.common;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.metricpublisher.MicrometerMetricPublisher;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

/**
 * A common factory to create SQS clients
 */
public final class SqsClientFactory {

    private SqsClientFactory() {
    }

    public static SqsClient createSqsClient(
            final Region region,
            final AwsCredentialsProvider credentialsProvider) {

        final PluginMetrics awsSdkMetrics = PluginMetrics.fromNames("sdk", "aws");

        return SqsClient.builder()
                .region(region)
                .credentialsProvider(credentialsProvider)
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .retryPolicy(RetryPolicy.builder().numRetries(5).build())
                        .addMetricPublisher(new MicrometerMetricPublisher(awsSdkMetrics))
                        .build())
                .build();
    }
}
