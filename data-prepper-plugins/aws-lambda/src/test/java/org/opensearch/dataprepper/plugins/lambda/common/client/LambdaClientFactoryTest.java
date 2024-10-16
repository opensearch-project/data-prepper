package org.opensearch.dataprepper.plugins.lambda.common.client;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import static org.mockito.ArgumentMatchers.any;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.lambda.common.config.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.metricpublisher.MicrometerMetricPublisher;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.LambdaAsyncClientBuilder;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@ExtendWith(MockitoExtension.class)
class LambdaClientFactoryTest {
    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @Mock
    private AwsCredentialsProvider awsCredentialsProvider;

    private Duration sdkTimeout = Duration.ofSeconds(60);

    @Test
    void createLambdaAsyncClient_with_real_LambdaAsyncClient() {
        try (MockedStatic<LambdaAsyncClient> mockedStaticLambdaAsyncClient = mockStatic(LambdaAsyncClient.class);
             MockedStatic<PluginMetrics> mockedPluginMetrics = mockStatic(PluginMetrics.class)) {

            PluginMetrics pluginMetricsMock = mock(PluginMetrics.class);
            mockedPluginMetrics.when(() -> PluginMetrics.fromNames("sdk", "aws")).thenReturn(pluginMetricsMock);

            LambdaAsyncClientBuilder lambdaAsyncClientBuilder = mock(LambdaAsyncClientBuilder.class);
            LambdaAsyncClient lambdaAsyncClientMock = mock(LambdaAsyncClient.class);

            mockedStaticLambdaAsyncClient.when(LambdaAsyncClient::builder).thenReturn(lambdaAsyncClientBuilder);

            when(lambdaAsyncClientBuilder.region(any(Region.class))).thenReturn(lambdaAsyncClientBuilder);
            when(lambdaAsyncClientBuilder.credentialsProvider(any(AwsCredentialsProvider.class))).thenReturn(lambdaAsyncClientBuilder);
            when(lambdaAsyncClientBuilder.overrideConfiguration(any(ClientOverrideConfiguration.class))).thenReturn(lambdaAsyncClientBuilder);
            when(lambdaAsyncClientBuilder.build()).thenReturn(lambdaAsyncClientMock);

            Region region = Region.US_WEST_2;
            when(awsAuthenticationOptions.getAwsRegion()).thenReturn(region);
            String stsRoleArn = UUID.randomUUID().toString();
            String stsExternalId = UUID.randomUUID().toString();
            Map<String, String> stsHeaderOverrides = Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
            when(awsAuthenticationOptions.getAwsStsRoleArn()).thenReturn(stsRoleArn);
            when(awsAuthenticationOptions.getAwsStsExternalId()).thenReturn(stsExternalId);
            when(awsAuthenticationOptions.getAwsStsHeaderOverrides()).thenReturn(stsHeaderOverrides);
            when(awsCredentialsSupplier.getProvider(any(AwsCredentialsOptions.class))).thenReturn(awsCredentialsProvider);

            // Act
            int maxConnectionRetries = 3;
            LambdaAsyncClient lambdaAsyncClient = LambdaClientFactory.createAsyncLambdaClient(
                    awsAuthenticationOptions,
                    maxConnectionRetries,
                    awsCredentialsSupplier,
                    sdkTimeout
            );

            // Verify
            assertThat(lambdaAsyncClient, notNullValue());
            verify(lambdaAsyncClientBuilder).region(region);
            verify(lambdaAsyncClientBuilder).credentialsProvider(awsCredentialsProvider);
            // Capture and verify ClientOverrideConfiguration
            ArgumentCaptor<ClientOverrideConfiguration> configCaptor = ArgumentCaptor.forClass(ClientOverrideConfiguration.class);
            verify(lambdaAsyncClientBuilder).overrideConfiguration(configCaptor.capture());
            ClientOverrideConfiguration config = configCaptor.getValue();

            assertThat(config.apiCallTimeout(), equalTo(Optional.of(sdkTimeout)));

            // Verify RetryPolicy
            assertThat(config.retryPolicy().isPresent(), equalTo(true));
            RetryPolicy retryPolicy = config.retryPolicy().get();
            assertThat(retryPolicy.numRetries(), equalTo(maxConnectionRetries));

            // Verify MetricPublisher
            assertThat(config.metricPublishers(), notNullValue());
            assertThat(config.metricPublishers().size(), equalTo(1));
            MetricPublisher metricPublisher = config.metricPublishers().get(0);
            assertThat(metricPublisher, instanceOf(MicrometerMetricPublisher.class));

            // Verify that awsCredentialsSupplier.getProvider was called with correct AwsCredentialsOptions
            ArgumentCaptor<AwsCredentialsOptions> optionsCaptor = ArgumentCaptor.forClass(AwsCredentialsOptions.class);
            verify(awsCredentialsSupplier).getProvider(optionsCaptor.capture());
            AwsCredentialsOptions credentialsOptions = optionsCaptor.getValue();
            assertThat(credentialsOptions.getRegion(), equalTo(region));
            assertThat(credentialsOptions.getStsRoleArn(), equalTo(stsRoleArn));
            assertThat(credentialsOptions.getStsExternalId(), equalTo(stsExternalId));
            assertThat(credentialsOptions.getStsHeaderOverrides(), equalTo(stsHeaderOverrides));
        }
    }

    @Test
    void createAsyncLambdaClient_with_correct_configuration() {
        try (MockedStatic<LambdaAsyncClient> mockedStaticLambdaAsyncClient = mockStatic(LambdaAsyncClient.class);
             MockedStatic<PluginMetrics> mockedPluginMetrics = mockStatic(PluginMetrics.class)) {

            PluginMetrics pluginMetricsMock = mock(PluginMetrics.class);
            mockedPluginMetrics.when(() -> PluginMetrics.fromNames("sdk", "aws")).thenReturn(pluginMetricsMock);

            LambdaAsyncClientBuilder lambdaAsyncClientBuilder = mock(LambdaAsyncClientBuilder.class);
            LambdaAsyncClient lambdaAsyncClientMock = mock(LambdaAsyncClient.class);

            mockedStaticLambdaAsyncClient.when(LambdaAsyncClient::builder).thenReturn(lambdaAsyncClientBuilder);

            when(lambdaAsyncClientBuilder.region(any(Region.class))).thenReturn(lambdaAsyncClientBuilder);
            when(lambdaAsyncClientBuilder.credentialsProvider(any(AwsCredentialsProvider.class))).thenReturn(lambdaAsyncClientBuilder);
            when(lambdaAsyncClientBuilder.overrideConfiguration(any(ClientOverrideConfiguration.class))).thenReturn(lambdaAsyncClientBuilder);
            when(lambdaAsyncClientBuilder.build()).thenReturn(lambdaAsyncClientMock);

            Region region = Region.US_WEST_2;
            when(awsAuthenticationOptions.getAwsRegion()).thenReturn(region);
            String stsRoleArn = UUID.randomUUID().toString();
            String stsExternalId = UUID.randomUUID().toString();
            Map<String, String> stsHeaderOverrides = Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
            when(awsAuthenticationOptions.getAwsStsRoleArn()).thenReturn(stsRoleArn);
            when(awsAuthenticationOptions.getAwsStsExternalId()).thenReturn(stsExternalId);
            when(awsAuthenticationOptions.getAwsStsHeaderOverrides()).thenReturn(stsHeaderOverrides);
            when(awsCredentialsSupplier.getProvider(any(AwsCredentialsOptions.class))).thenReturn(awsCredentialsProvider);

            // Act
            int maxConnectionRetries = 3;
            LambdaAsyncClient lambdaAsyncClient = LambdaClientFactory.createAsyncLambdaClient(
                    awsAuthenticationOptions,
                    maxConnectionRetries,
                    awsCredentialsSupplier,
                    sdkTimeout
            );

            // Verify
            assertThat(lambdaAsyncClient, notNullValue());

            // Verify builder methods
            verify(lambdaAsyncClientBuilder).region(region);
            verify(lambdaAsyncClientBuilder).credentialsProvider(awsCredentialsProvider);

            // Capture and verify ClientOverrideConfiguration
            ArgumentCaptor<ClientOverrideConfiguration> configCaptor = ArgumentCaptor.forClass(ClientOverrideConfiguration.class);
            verify(lambdaAsyncClientBuilder).overrideConfiguration(configCaptor.capture());
            ClientOverrideConfiguration config = configCaptor.getValue();

            // Verify apiCallTimeout
            assertThat(config.apiCallTimeout(), equalTo(Optional.of(sdkTimeout)));

            // Verify RetryPolicy
            assertThat(config.retryPolicy().isPresent(), equalTo(true));
            RetryPolicy retryPolicy = config.retryPolicy().get();
            assertThat(retryPolicy.numRetries(), equalTo(maxConnectionRetries));

            // Verify MetricPublisher
            assertThat(config.metricPublishers(), notNullValue());
            assertThat(config.metricPublishers().size(), equalTo(1));
            MetricPublisher metricPublisher = config.metricPublishers().get(0);
            assertThat(metricPublisher, instanceOf(MicrometerMetricPublisher.class));

            // Verify that awsCredentialsSupplier.getProvider was called with correct AwsCredentialsOptions
            ArgumentCaptor<AwsCredentialsOptions> optionsCaptor = ArgumentCaptor.forClass(AwsCredentialsOptions.class);
            verify(awsCredentialsSupplier).getProvider(optionsCaptor.capture());
            AwsCredentialsOptions credentialsOptions = optionsCaptor.getValue();
            assertThat(credentialsOptions.getRegion(), equalTo(region));
            assertThat(credentialsOptions.getStsRoleArn(), equalTo(stsRoleArn));
            assertThat(credentialsOptions.getStsExternalId(), equalTo(stsExternalId));
            assertThat(credentialsOptions.getStsHeaderOverrides(), equalTo(stsHeaderOverrides));
        }
    }
}
