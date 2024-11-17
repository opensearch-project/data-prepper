package org.opensearch.dataprepper.plugins.lambda.common.client;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.lambda.common.config.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.ClientOptions;
import org.opensearch.dataprepper.plugins.metricpublisher.MicrometerMetricPublisher;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.backoff.EqualJitterBackoffStrategy;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;

public final class LambdaClientFactory {

  public static LambdaAsyncClient createAsyncLambdaClient(
          final AwsAuthenticationOptions awsAuthenticationOptions,
          final AwsCredentialsSupplier awsCredentialsSupplier,
          ClientOptions clientOptions) {
    final AwsCredentialsOptions awsCredentialsOptions = convertToCredentialsOptions(
            awsAuthenticationOptions);
    final AwsCredentialsProvider awsCredentialsProvider = awsCredentialsSupplier.getProvider(
            awsCredentialsOptions);
    final PluginMetrics awsSdkMetrics = PluginMetrics.fromNames("sdk", "aws");

    return LambdaAsyncClient.builder()
            .region(awsAuthenticationOptions.getAwsRegion())
            .credentialsProvider(awsCredentialsProvider)
            .overrideConfiguration(
                    createOverrideConfiguration(clientOptions, awsSdkMetrics))
            .httpClient(NettyNioAsyncHttpClient.builder()
                    .maxConcurrency(clientOptions.getMaxConcurrency())
                    .connectionTimeout(clientOptions.getConnectionTimeout()).build())
            .build();
  }

  private static ClientOverrideConfiguration createOverrideConfiguration(
          ClientOptions clientOptions,
          final PluginMetrics awsSdkMetrics) {

    //TODO - Add AdaptiveRetryStrategy
    //https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/retry-strategy.html
    BackoffStrategy backoffStrategy = EqualJitterBackoffStrategy.builder()
            .baseDelay(clientOptions.getBaseDelay())
            .maxBackoffTime(clientOptions.getMaxBackoff())
            .build();

    final RetryPolicy retryPolicy = RetryPolicy.builder()
            .numRetries(clientOptions.getMaxConnectionRetries())
            .backoffStrategy(backoffStrategy)
            .build();

    return ClientOverrideConfiguration.builder()
            .retryPolicy(retryPolicy)
            .addMetricPublisher(new MicrometerMetricPublisher(awsSdkMetrics))
            .apiCallTimeout(clientOptions.getApiCallTimeout())
            .build();
  }

  private static AwsCredentialsOptions convertToCredentialsOptions(
          final AwsAuthenticationOptions awsAuthenticationOptions) {
    return AwsCredentialsOptions.builder()
            .withRegion(awsAuthenticationOptions.getAwsRegion())
            .withStsRoleArn(awsAuthenticationOptions.getAwsStsRoleArn())
            .withStsExternalId(awsAuthenticationOptions.getAwsStsExternalId())
            .withStsHeaderOverrides(awsAuthenticationOptions.getAwsStsHeaderOverrides())
            .build();
  }
}