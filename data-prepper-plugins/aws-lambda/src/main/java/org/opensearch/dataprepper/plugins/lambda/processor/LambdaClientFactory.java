/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.processor;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.lambda.common.config.AwsAuthenticationOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.services.lambda.LambdaClient;

public final class LambdaClientFactory {
    private LambdaClientFactory() {
    }

    public static LambdaClient createLambdaClient(final LambdaProcessorConfig lambdaProcessorConfig, final AwsCredentialsSupplier awsCredentialsSupplier) {
        final AwsCredentialsOptions awsCredentialsOptions = convertToCredentialsOptions(lambdaProcessorConfig.getAwsAuthenticationOptions());
        final AwsCredentialsProvider awsCredentialsProvider = awsCredentialsSupplier.getProvider(awsCredentialsOptions);

        return LambdaClient.builder().region(lambdaProcessorConfig.getAwsAuthenticationOptions().getAwsRegion()).credentialsProvider(awsCredentialsProvider).overrideConfiguration(createOverrideConfiguration(lambdaProcessorConfig)).build();

    }

    private static ClientOverrideConfiguration createOverrideConfiguration(final LambdaProcessorConfig lambdaProcessorConfig) {
        final RetryPolicy retryPolicy = RetryPolicy.builder().numRetries(lambdaProcessorConfig.getMaxConnectionRetries()).build();
        return ClientOverrideConfiguration.builder().retryPolicy(retryPolicy).build();
    }

    private static AwsCredentialsOptions convertToCredentialsOptions(final AwsAuthenticationOptions awsAuthenticationOptions) {
        return AwsCredentialsOptions.builder().withRegion(awsAuthenticationOptions.getAwsRegion()).withStsRoleArn(awsAuthenticationOptions.getAwsStsRoleArn()).withStsExternalId(awsAuthenticationOptions.getAwsStsExternalId()).withStsHeaderOverrides(awsAuthenticationOptions.getAwsStsHeaderOverrides()).build();
    }
}
