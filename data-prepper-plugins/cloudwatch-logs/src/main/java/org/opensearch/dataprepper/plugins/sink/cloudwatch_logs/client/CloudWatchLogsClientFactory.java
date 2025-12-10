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
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClientBuilder;

import java.net.URI;

import java.util.Map;

/**
 * CwlClientFactory is in charge of reading in aws config parameters to return a working
 * client for interfacing with CloudWatchLogs services.
 */
public final class CloudWatchLogsClientFactory {

    private CloudWatchLogsClientFactory() {
    }

    /**
     * Generates a CloudWatchLogs Client based on STS role ARN system credentials.
     *
     * @param awsConfig              AwsConfig specifying region, roles, and header overrides.
     * @param awsCredentialsSupplier AwsCredentialsSupplier Interface for which to create CredentialsProvider for Client config.
     * @param customHeaders          Map of custom headers to include in requests. Can be null.
     * @param endpoint               Optional endpoint URL to override the default CloudWatch Logs endpoint.
     * @return CloudWatchLogsClient used to interact with CloudWatch Logs services.
     */
    public static CloudWatchLogsClient createCwlClient(final AwsConfig awsConfig,
            final AwsCredentialsSupplier awsCredentialsSupplier,
            final Map<String, String> customHeaders,
            final String endpoint) {
        final AwsCredentialsOptions awsCredentialsOptions = awsConfig != null
                ? convertToCredentialOptions(awsConfig)
                : AwsCredentialsOptions.defaultOptions();
        final AwsCredentialsProvider awsCredentialsProvider = awsCredentialsSupplier.getProvider(awsCredentialsOptions);
        Region region = awsConfig != null ? awsConfig.getAwsRegion() : awsCredentialsSupplier.getDefaultRegion().get();

        if (awsCredentialsProvider == null || region == null) {
            return null;
        }

        CloudWatchLogsClientBuilder clientBuilder = CloudWatchLogsClient.builder()
                .region(region)
                .credentialsProvider(awsCredentialsProvider)
                .overrideConfiguration(createOverrideConfiguration(customHeaders));
        
        if (endpoint != null && !endpoint.trim().isEmpty()) {
            clientBuilder.endpointOverride(URI.create(endpoint));
        }
        
        return clientBuilder.build();
    }

    private static ClientOverrideConfiguration createOverrideConfiguration(final Map<String, String> customHeaders) {
        final RetryPolicy retryPolicy = RetryPolicy.builder()
                .numRetries(AwsConfig.DEFAULT_CONNECTION_ATTEMPTS)
                .build();

        final ClientOverrideConfiguration.Builder configBuilder = ClientOverrideConfiguration.builder()
                .retryPolicy(retryPolicy);

        customHeaders.forEach(configBuilder::putHeader);

        return configBuilder.build();
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
