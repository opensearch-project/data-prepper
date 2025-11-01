/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sourcecoordinator.dynamodb;

import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.backoff.EqualJitterBackoffStrategy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import javax.inject.Named;
import java.time.Duration;
import java.util.UUID;

@Named
public class DynamoDbClientFactory {

    /**
     * Set to infinite retries for ProvisionedThroughputExceededException.
     * All other retryable exceptions will retry 10 times due to {@link DdbClientCustomRetryCondition}
     */
    private static final int DYNAMO_CLIENT_RETRIES = Integer.MAX_VALUE;
    private static final long DYNAMO_CLIENT_BASE_BACKOFF_MILLIS = 1000L;
    private static final long DYNAMO_CLIENT_MAX_BACKOFF_MILLIS = 60000L;

    DynamoDbClient provideDynamoDbClient(
        final String region, final String stsRoleArn, final String stsExternalId
    ) {
        return DynamoDbClient.builder()
                .region(Region.of(region))
                .credentialsProvider(getAwsCredentials(Region.of(region), stsRoleArn, stsExternalId))
                .overrideConfiguration(getClientOverrideConfiguration())
                .build();
    }

    private static ClientOverrideConfiguration getClientOverrideConfiguration() {
        final BackoffStrategy backoffStrategy = EqualJitterBackoffStrategy.builder()
                .baseDelay(Duration.ofMillis(DYNAMO_CLIENT_BASE_BACKOFF_MILLIS))
                .maxBackoffTime(Duration.ofMillis(DYNAMO_CLIENT_MAX_BACKOFF_MILLIS))
                .build();

        final RetryPolicy retryPolicy = RetryPolicy.builder()
                .numRetries(DYNAMO_CLIENT_RETRIES)
                .retryCondition(new DdbClientCustomRetryCondition())
                .backoffStrategy(backoffStrategy)
                .throttlingBackoffStrategy(backoffStrategy)
                .build();

        return ClientOverrideConfiguration.builder()
                .retryPolicy(retryPolicy)
                .build();
    }

    private static AwsCredentialsProvider getAwsCredentials(
        final Region region, final String stsRoleArn, final String stsExternalId
    ) {
        AwsCredentialsProvider awsCredentialsProvider;
        if (stsRoleArn != null && !stsRoleArn.isEmpty()) {
            try {
                Arn.fromString(stsRoleArn);
            } catch (final Exception e) {
                throw new IllegalArgumentException("Invalid ARN format for dynamodb sts_role_arn");
            }

            final StsClient stsClient = StsClient.builder()
                    .region(region)
                    .overrideConfiguration(getClientOverrideConfiguration())
                    .build();

            AssumeRoleRequest.Builder assumeRoleRequestBuilder = AssumeRoleRequest.builder()
                    .roleSessionName("Dynamo-Source-Coordination-" + UUID.randomUUID())
                    .roleArn(stsRoleArn);

            if (stsExternalId != null && !stsExternalId.isEmpty()) {
                assumeRoleRequestBuilder = assumeRoleRequestBuilder.externalId(stsExternalId);
            }

            awsCredentialsProvider = StsAssumeRoleCredentialsProvider.builder()
                    .stsClient(stsClient)
                    .refreshRequest(assumeRoleRequestBuilder.build())
                    .build();

        } else {
            // use default credential provider
            awsCredentialsProvider = DefaultCredentialsProvider.create();
        }

        return awsCredentialsProvider;
    }
}
