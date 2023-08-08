/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.sink.s3.configuration.AwsAuthenticationOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.services.s3.S3Client;

public final class ClientFactory {
    private ClientFactory() { }

    static S3Client createS3Client(final S3SinkConfig s3SinkConfig, final AwsCredentialsSupplier awsCredentialsSupplier) {
        final AwsCredentialsOptions awsCredentialsOptions = convertToCredentialsOptions(s3SinkConfig.getAwsAuthenticationOptions());
        final AwsCredentialsProvider awsCredentialsProvider = awsCredentialsSupplier.getProvider(awsCredentialsOptions);

        return S3Client.builder()
                .region(s3SinkConfig.getAwsAuthenticationOptions().getAwsRegion())
                .credentialsProvider(awsCredentialsProvider)
                .overrideConfiguration(createOverrideConfiguration(s3SinkConfig)).build();
    }

    private static ClientOverrideConfiguration createOverrideConfiguration(final S3SinkConfig s3SinkConfig) {
        final RetryPolicy retryPolicy = RetryPolicy.builder().numRetries(s3SinkConfig.getMaxConnectionRetries()).build();
        return ClientOverrideConfiguration.builder()
                .retryPolicy(retryPolicy)
                .build();
    }

    private static AwsCredentialsOptions convertToCredentialsOptions(final AwsAuthenticationOptions awsAuthenticationOptions) {
        return AwsCredentialsOptions.builder()
            .withRegion(awsAuthenticationOptions.getAwsRegion())
            .withStsRoleArn(awsAuthenticationOptions.getAwsStsRoleArn())
            .withStsExternalId(awsAuthenticationOptions.getAwsStsExternalId())
            .withStsHeaderOverrides(awsAuthenticationOptions.getAwsStsHeaderOverrides())
            .build();
    }
}
