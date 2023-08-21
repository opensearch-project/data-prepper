/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.aws;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.arns.Arn;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.backoff.EqualJitterBackoffStrategy;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

class CredentialsProviderFactory {
    private static final Logger LOG = LoggerFactory.getLogger(CredentialsProviderFactory.class);
    private static final String AWS_IAM = "iam";
    private static final String AWS_IAM_ROLE = "role";
    static final int STS_CLIENT_RETRIES = 10;
    static final long STS_CLIENT_BASE_BACKOFF_MILLIS = 1000L;
    static final long STS_CLIENT_MAX_BACKOFF_MILLIS = 60000L;

    AwsCredentialsProvider providerFromOptions(final AwsCredentialsOptions credentialsOptions) {
        Objects.requireNonNull(credentialsOptions);

        if(credentialsOptions.getStsRoleArn() != null) {
            return createStsCredentials(credentialsOptions);
        }

        return DefaultCredentialsProvider.create();
    }

    private AwsCredentialsProvider createStsCredentials(final AwsCredentialsOptions credentialsOptions) {

        final String stsRoleArn = credentialsOptions.getStsRoleArn();

        validateStsRoleArn(stsRoleArn);

        LOG.debug("Creating new AwsCredentialsProvider with role {}.", stsRoleArn);

        final StsClient stsClient = createStsClient(credentialsOptions.getRegion());

        AssumeRoleRequest.Builder assumeRoleRequestBuilder = AssumeRoleRequest.builder()
                .roleSessionName("Data-Prepper-" + UUID.randomUUID())
                .roleArn(stsRoleArn);

        if (credentialsOptions.getStsExternalId() != null && !credentialsOptions.getStsExternalId().isEmpty()) {
            assumeRoleRequestBuilder = assumeRoleRequestBuilder.externalId(credentialsOptions.getStsExternalId());
        }

        final Map<String, String> awsStsHeaderOverrides = credentialsOptions.getStsHeaderOverrides();

        if(awsStsHeaderOverrides != null && !awsStsHeaderOverrides.isEmpty()) {
            assumeRoleRequestBuilder = assumeRoleRequestBuilder
                    .overrideConfiguration(configuration -> awsStsHeaderOverrides.forEach(configuration::putHeader));
        }

        return StsAssumeRoleCredentialsProvider.builder()
                .stsClient(stsClient)
                .refreshRequest(assumeRoleRequestBuilder.build())
                .build();
    }

    private void validateStsRoleArn(final String stsRoleArn) {
        final Arn arn = getArn(stsRoleArn);
        if (!AWS_IAM.equals(arn.service())) {
            throw new IllegalArgumentException("sts_role_arn must be an IAM Role");
        }
        final Optional<String> resourceType = arn.resource().resourceType();
        if (resourceType.isEmpty() || !resourceType.get().equals(AWS_IAM_ROLE)) {
            throw new IllegalArgumentException("sts_role_arn must be an IAM Role");
        }
    }

    private Arn getArn(final String stsRoleArn) {
        try {
            return Arn.fromString(stsRoleArn);
        } catch (final Exception e) {
            throw new IllegalArgumentException(String.format("Invalid ARN format for awsStsRoleArn. Check the format of %s", stsRoleArn));
        }
    }

    private StsClient createStsClient(final Region region) {
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

        StsClientBuilder stsClientBuilder = StsClient.builder()
                .overrideConfiguration(clientOverrideConfiguration);

        stsClientBuilder = Optional.ofNullable(region)
                .map(stsClientBuilder::region)
                .orElse(stsClientBuilder);

        return stsClientBuilder.build();
    }
}
