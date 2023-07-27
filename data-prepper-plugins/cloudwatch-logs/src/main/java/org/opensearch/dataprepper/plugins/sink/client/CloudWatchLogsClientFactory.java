/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.client;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.sink.config.AwsConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;

/**
 * CwlClientFactory is in charge of reading in
 * aws config parameters to return a working
 * client for interfacing with
 * CloudWatchLogs services.
 */
public final class CloudWatchLogsClientFactory {

    /**
     * Generates a CloudWatchLogs Client based on STS role ARN system credentials.
     * @param awsConfig - AwsConfig specifying region, roles, and header overrides.
     * @param awsCredentialsSupplier - AwsCredentialsSupplier Interface for which to create CredentialsProvider for Client config.
     * @return CloudWatchLogsClient - used to interact with CloudWatch Logs services.
     */
    public static CloudWatchLogsClient createCwlClient(final AwsConfig awsConfig, final AwsCredentialsSupplier awsCredentialsSupplier) {
        final AwsCredentialsOptions awsCredentialsOptions = convertToCredentialOptions(awsConfig);
        final AwsCredentialsProvider awsCredentialsProvider = awsCredentialsSupplier.getProvider(awsCredentialsOptions);

        return CloudWatchLogsClient.builder()
                .region(awsConfig.getAwsRegion())
                .credentialsProvider(awsCredentialsProvider)
                .overrideConfiguration(createOverrideConfiguration()).build();
    }

    private static ClientOverrideConfiguration createOverrideConfiguration() {
        final RetryPolicy retryPolicy = RetryPolicy.builder().numRetries(AwsConfig.DEFAULT_CONNECTION_ATTEMPTS).build();

        return ClientOverrideConfiguration.builder()
                .retryPolicy(retryPolicy)
                .build();
    }

    private static AwsCredentialsOptions convertToCredentialOptions(final AwsConfig awsConfig) {
        return AwsCredentialsOptions.builder()
                .withRegion(awsConfig.getAwsRegion())
                .withStsRoleArn(awsConfig.getAwsStsRoleArn())
                .withStsExternalId(awsConfig.getAwsStsExternalId())
                .withStsHeaderOverrides(awsConfig.getAwsStsHeaderOverrides())
                .build();
    }
}
