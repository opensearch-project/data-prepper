package org.opensearch.dataprepper.plugins.sink.client;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.sink.config.AwsConfig;
import org.opensearch.dataprepper.plugins.sink.config.CwlSinkConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;

/**
 * CwlClientFactory is in charge of reading in
 * aws config parameters to return a working
 * client for interfacing with
 * CloudWatchLogs services.
 * //TODO: Test these methods to ensure that they are working and we are receiving information.
 */
public final class CwlClientFactory {

    /**
     * Generates a CloudWatchLogs Client based on STS role ARN system credentials.
     * @return CloudWatchLogsClient -> used to interact with CloudWatch Logs services.
     */
    static CloudWatchLogsClient createCwlClient(final AwsConfig awsConfig, final AwsCredentialsSupplier awsCredentialsSupplier) {
        final AwsCredentialsOptions awsCredentialsOptions = convertToCredentialOptions(awsConfig);
        final AwsCredentialsProvider awsCredentialsProvider = awsCredentialsSupplier.getProvider(awsCredentialsOptions);

        return CloudWatchLogsClient.builder()
                .region(awsConfig.getAwsRegion())
                .credentialsProvider(awsCredentialsProvider)
                .overrideConfiguration(createOverrideConfiguration(awsConfig)).build();
    }

    /**
     * Generates a CloudWatchLogs Client based on default system credentials.
     * @return CloudWatchLogsClient -> used to interact with CloudWatch Logs services.
     */
    static CloudWatchLogsClient createCwlClient() {
        return CloudWatchLogsClient.builder()
                .credentialsProvider(ProfileCredentialsProvider.create())
                .build();
    }

    /**
     * Generates a CloudWatchLogs Client based on specified profile system credentials.
     * @return CloudWatchLogsClient -> used to interact with CloudWatch Logs services.
     */
    static CloudWatchLogsClient createCwlClient(final AwsConfig awsConfig) {
        final AwsCredentialsProvider awsCredentialsProvider = ProfileCredentialsProvider.builder().profileName(awsConfig.getPathToCredentials()).build();

        return CloudWatchLogsClient.builder()
                .region(awsConfig.getAwsRegion())
                .credentialsProvider(awsCredentialsProvider)
                .build();
    }

    private static ClientOverrideConfiguration createOverrideConfiguration(final AwsConfig awsConfig) {
        final RetryPolicy retryPolicy = RetryPolicy.builder().numRetries(awsConfig.getDEFAULT_CONNECTION_ATTEMPTS()).build();

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
