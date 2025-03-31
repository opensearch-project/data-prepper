/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.client;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config.AwsConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;

/**
 * CwlClientFactory is in charge of reading in aws config parameters to return a working
 * client for interfacing with CloudWatchLogs services.
 */
public final class CloudWatchLogsClientFactory {
    private CloudWatchLogsClientFactory() {
    }

    /**
     * Generates a CloudWatchLogs Client based on STS role ARN system credentials.
     * @param awsConfig AwsConfig specifying region, roles, and header overrides.
     * @param awsCredentialsSupplier AwsCredentialsSupplier Interface for which to create CredentialsProvider for Client config.
     * @return CloudWatchLogsClient used to interact with CloudWatch Logs services.
     */
    public static CloudWatchLogsClient createCwlClient(final AwsConfig awsConfig, final AwsCredentialsSupplier awsCredentialsSupplier) {
        final AwsCredentialsProvider awsCredentialsProvider = awsConfig != null ? awsCredentialsSupplier.getProvider(convertToCredentialOptions(awsConfig)) : awsCredentialsSupplier.getProvider(AwsCredentialsOptions.builder().build());
        Region region = awsConfig != null ? awsConfig.getAwsRegion() : awsCredentialsSupplier.getDefaultRegion().get();

        if (awsCredentialsProvider == null || region == null) {
            return null;
        }
        return CloudWatchLogsClient.builder()
                .region(region)
                .credentialsProvider(awsCredentialsProvider)
                .overrideConfiguration(createOverrideConfiguration()).build();
    }

    private static ClientOverrideConfiguration createOverrideConfiguration() {
        return ClientOverrideConfiguration.builder()
                .retryPolicy(r -> r.numRetries(AwsConfig.DEFAULT_CONNECTION_ATTEMPTS))
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
