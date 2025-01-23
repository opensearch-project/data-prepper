package org.opensearch.dataprepper.plugins.source.sqs.common;

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

        return SqsClient.builder()
                .region(region)
                .credentialsProvider(credentialsProvider)
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .retryPolicy(RetryPolicy.builder().numRetries(5).build())
                        .build())
                .build();
    }
}
