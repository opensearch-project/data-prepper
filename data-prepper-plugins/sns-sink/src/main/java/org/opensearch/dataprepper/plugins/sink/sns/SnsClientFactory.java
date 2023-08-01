/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.sns;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.sink.sns.configuration.AwsAuthenticationOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.services.sns.SnsClient;

public class SnsClientFactory {

    public static SnsClient createSNSClient(final SnsSinkConfig snsSinkConfig,
                                     final AwsCredentialsSupplier awsCredentialsSupplier) {
        final AwsCredentialsOptions awsCredentialsOptions = convertToCredentialsOptions(snsSinkConfig.getAwsAuthenticationOptions());
        final AwsCredentialsProvider awsCredentialsProvider = awsCredentialsSupplier.getProvider(awsCredentialsOptions);

        return SnsClient.builder()
                .region(snsSinkConfig.getAwsAuthenticationOptions().getAwsRegion())
                .credentialsProvider(awsCredentialsProvider)
                .overrideConfiguration(createOverrideConfiguration(snsSinkConfig)).build();
    }

    private static ClientOverrideConfiguration createOverrideConfiguration(final SnsSinkConfig snsSinkConfig) {
        final RetryPolicy retryPolicy = RetryPolicy.builder().numRetries(snsSinkConfig.getMaxConnectionRetries()).build();
        return ClientOverrideConfiguration.builder()
                .retryPolicy(retryPolicy)
                .build();
    }

    private static AwsCredentialsOptions convertToCredentialsOptions(final AwsAuthenticationOptions awsAuthenticationOptions) {
        return AwsCredentialsOptions.builder()
                .withRegion(awsAuthenticationOptions.getAwsRegion())
                .withStsRoleArn(awsAuthenticationOptions.getAwsStsRoleArn())
                .withStsHeaderOverrides(awsAuthenticationOptions.getAwsStsHeaderOverrides())
                .build();
    }
}
