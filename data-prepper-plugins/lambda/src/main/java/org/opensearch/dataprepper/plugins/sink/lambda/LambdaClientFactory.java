/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.lambda;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.common.lambda.config.AwsAuthenticationOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.services.lambda.LambdaClient;

public final class LambdaClientFactory {
    private LambdaClientFactory() { }

    static LambdaClient createLambdaClient(final LambdaSinkConfig lambdaSinkConfig,
                                           final AwsCredentialsSupplier awsCredentialsSupplier) {
         final AwsCredentialsOptions awsCredentialsOptions = convertToCredentialsOptions(lambdaSinkConfig.getAwsAuthenticationOptions());
         final AwsCredentialsProvider awsCredentialsProvider = awsCredentialsSupplier.getProvider(awsCredentialsOptions);

            return LambdaClient.builder()
                .region(lambdaSinkConfig.getAwsAuthenticationOptions().getAwsRegion())
                .credentialsProvider(awsCredentialsProvider)
                .overrideConfiguration(createOverrideConfiguration(lambdaSinkConfig)).build();

    }

    private static ClientOverrideConfiguration createOverrideConfiguration(final LambdaSinkConfig lambdaSinkConfig) {
        final RetryPolicy retryPolicy = RetryPolicy.builder().numRetries(lambdaSinkConfig.getMaxConnectionRetries()).build();
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
