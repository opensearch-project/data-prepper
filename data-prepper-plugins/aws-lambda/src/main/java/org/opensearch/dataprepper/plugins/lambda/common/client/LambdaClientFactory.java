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
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;

import java.time.Duration;

public final class LambdaClientFactory {
    private LambdaClientFactory() { }

    public static LambdaAsyncClient createAsyncLambdaClient(final AwsAuthenticationOptions awsAuthenticationOptions,
                                                            final int maxConnectionRetries,
                                                            final AwsCredentialsSupplier awsCredentialsSupplier,
                                                            final Duration sdkTimeout) {
        final AwsCredentialsOptions awsCredentialsOptions = convertToCredentialsOptions(awsAuthenticationOptions);
        final AwsCredentialsProvider awsCredentialsProvider = awsCredentialsSupplier.getProvider(awsCredentialsOptions);
        final PluginMetrics awsSdkMetrics = PluginMetrics.fromNames("sdk", "aws");

        return LambdaAsyncClient.builder()
                .region(awsAuthenticationOptions.getAwsRegion())
                .credentialsProvider(awsCredentialsProvider)
                .overrideConfiguration(createOverrideConfiguration(maxConnectionRetries, awsSdkMetrics, sdkTimeout))
                .build();
    }

    private static ClientOverrideConfiguration createOverrideConfiguration(final int maxConnectionRetries,
                                                                           final PluginMetrics awsSdkMetrics,
                                                                           final Duration sdkTimeout) {
        final RetryPolicy retryPolicy = RetryPolicy.builder().numRetries(maxConnectionRetries).build();
        return ClientOverrideConfiguration.builder()
                .retryPolicy(retryPolicy)
                .addMetricPublisher(new MicrometerMetricPublisher(awsSdkMetrics))
                .apiCallTimeout(sdkTimeout) //default sdk limit is 60secs, requests to lambda might fail if lambda takes >60sec to process
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
