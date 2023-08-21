/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.s3;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.backoff.EqualJitterBackoffStrategy;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.time.Duration;
import java.util.UUID;

public class S3ClientProvider {
    private static final int STS_CLIENT_RETRIES = 10;
    private static final long STS_CLIENT_BASE_BACKOFF_MILLIS = 1000L;
    private static final long STS_CLIENT_MAX_BACKOFF_MILLIS = 60000L;

    private final String awsRegion;
    private final String awsStsRoleArn;
    private final String awsStsExternalId;
    private final ApacheHttpClient.Builder apacheHttpClientBuilder = ApacheHttpClient.builder();

    public S3ClientProvider(
        final String awsRegion,
        final String awsStsRoleArn,
        final String awsStsExternalId
    ) {
        this.awsRegion = awsRegion;
        this.awsStsRoleArn = awsStsRoleArn;
        this.awsStsExternalId = awsStsExternalId;
    }

    public S3Client buildS3Client() {
        final AwsCredentialsProvider credentialsProvider = buildAwsCredentialsProvider();

        return S3Client.builder()
                .region(Region.of(awsRegion))
                .credentialsProvider(credentialsProvider)
                .httpClientBuilder(apacheHttpClientBuilder)
                .build();
    }

    private AwsCredentialsProvider buildAwsCredentialsProvider() {
        AwsCredentialsProvider awsCredentialsProvider;
        if (awsStsRoleArn != null && !awsStsRoleArn.isEmpty()) {
            awsCredentialsProvider = StsAssumeRoleCredentialsProvider.builder()
                    .stsClient(getStsClient(awsRegion))
                    .refreshRequest(getAssumeRoleRequest(awsStsRoleArn))
                    .build();
        } else {
            awsCredentialsProvider = DefaultCredentialsProvider.create();
        }
        return awsCredentialsProvider;
    }

    private AssumeRoleRequest getAssumeRoleRequest(final String awsStsRoleArn) {
        AssumeRoleRequest.Builder builder = AssumeRoleRequest.builder()
                .roleSessionName("OpenSearch-Sink-S3" + UUID.randomUUID())
                .roleArn(awsStsRoleArn);

        if (awsStsExternalId != null && !awsStsExternalId.isEmpty()) {
            builder = builder.externalId(awsStsExternalId);
        }

        return builder.build();
    }

    private StsClient getStsClient(final String awsRegion) {
        final BackoffStrategy backoffStrategy = EqualJitterBackoffStrategy.builder()
                .baseDelay(Duration.ofMillis(STS_CLIENT_BASE_BACKOFF_MILLIS))
                .maxBackoffTime(Duration.ofMillis(STS_CLIENT_MAX_BACKOFF_MILLIS))
                .build();

        final RetryPolicy retryPolicy = RetryPolicy.builder()
                .numRetries(STS_CLIENT_RETRIES)
                .retryCondition(RetryCondition.defaultRetryCondition())
                .backoffStrategy(backoffStrategy)
                .throttlingBackoffStrategy(backoffStrategy)
                .build();

        final ClientOverrideConfiguration clientOverrideConfiguration = ClientOverrideConfiguration.builder()
                .retryPolicy(retryPolicy)
                .build();

        return StsClient.builder()
                .overrideConfiguration(clientOverrideConfiguration)
                .region(Region.of(awsRegion))
                .httpClientBuilder(apacheHttpClientBuilder)
                .build();
    }
}
