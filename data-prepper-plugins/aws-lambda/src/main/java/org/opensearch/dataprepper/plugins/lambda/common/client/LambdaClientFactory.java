/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.common.client;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.lambda.common.config.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.metricpublisher.MicrometerMetricPublisher;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.services.lambda.LambdaClient;

public final class LambdaClientFactory {
    private LambdaClientFactory() { }

    public static LambdaClient createLambdaClient(final AwsAuthenticationOptions awsAuthenticationOptions,
                                                  final int maxConnectionRetries,
                                                  final AwsCredentialsSupplier awsCredentialsSupplier) {
         final AwsCredentialsOptions awsCredentialsOptions = convertToCredentialsOptions(awsAuthenticationOptions);
         final AwsCredentialsProvider awsCredentialsProvider = awsCredentialsSupplier.getProvider(awsCredentialsOptions);
        final PluginMetrics awsSdkMetrics = PluginMetrics.fromNames("sdk", "aws");

            return LambdaClient.builder()
                    .region(awsAuthenticationOptions.getAwsRegion())
                    .credentialsProvider(awsCredentialsProvider)
                    .overrideConfiguration(createOverrideConfiguration(maxConnectionRetries,awsSdkMetrics))
                    .build();

    }

    private static ClientOverrideConfiguration createOverrideConfiguration(final int maxConnectionRetries,
                                                                           final PluginMetrics awsSdkMetrics) {
        final RetryPolicy retryPolicy = RetryPolicy.builder().numRetries(maxConnectionRetries).build();
        return ClientOverrideConfiguration.builder()
                .retryPolicy(retryPolicy)
                .addMetricPublisher(new MicrometerMetricPublisher(awsSdkMetrics))
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
