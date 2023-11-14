package org.opensearch.dataprepper.plugins.common.opensearch;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.sink.opensearch.ConnectionConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.AwsAuthenticationConfiguration;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy;
import software.amazon.awssdk.services.opensearchserverless.OpenSearchServerlessClient;

import java.time.Duration;

public class ServerlessNetworkPolicyUpdaterFactory {
    public static ServerlessNetworkPolicyUpdater create(
        final AwsCredentialsSupplier awsCredentialsSupplier,
        final ConnectionConfiguration connectionConfiguration
    ) {
        return new ServerlessNetworkPolicyUpdater(getOpenSearchServerlessClient(
            awsCredentialsSupplier, connectionConfiguration.createAwsCredentialsOptions()
        ));
    }

    public static ServerlessNetworkPolicyUpdater create(
        final AwsCredentialsSupplier awsCredentialsSupplier,
        final AwsAuthenticationConfiguration awsConfig
    ) {
        final AwsCredentialsOptions awsCredentialsOptions = AwsCredentialsOptions.builder()
            .withRegion(awsConfig.getAwsRegion())
            .withStsRoleArn(awsConfig.getAwsStsRoleArn())
            .withStsExternalId(awsConfig.getAwsStsExternalId())
            .withStsHeaderOverrides(awsConfig.getAwsStsHeaderOverrides())
            .build();

        return new ServerlessNetworkPolicyUpdater(getOpenSearchServerlessClient(
            awsCredentialsSupplier, awsCredentialsOptions
        ));
    }

    private static OpenSearchServerlessClient getOpenSearchServerlessClient(
        final AwsCredentialsSupplier awsCredentialsSupplier,
        final AwsCredentialsOptions awsCredentialsOptions
    ) {
        return OpenSearchServerlessClient.builder()
            .credentialsProvider(awsCredentialsSupplier.getProvider(awsCredentialsOptions))
            .region(awsCredentialsOptions.getRegion())
            .overrideConfiguration(ClientOverrideConfiguration.builder()
                .retryPolicy(RetryPolicy.builder()
                    .backoffStrategy(FullJitterBackoffStrategy.builder()
                        .baseDelay(Duration.ofSeconds(10))
                        .maxBackoffTime(Duration.ofSeconds(60))
                        .build())
                    .numRetries(10)
                    .build())
                .build())
            .build();
    }
}
