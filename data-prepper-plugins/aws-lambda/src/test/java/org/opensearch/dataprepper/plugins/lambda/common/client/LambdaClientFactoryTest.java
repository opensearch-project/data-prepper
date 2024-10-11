package org.opensearch.dataprepper.plugins.lambda.common.client;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import static org.mockito.ArgumentMatchers.any;
import org.mockito.Mock;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.lambda.common.config.AwsAuthenticationOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.LambdaClientBuilder;

import java.util.Map;
import java.util.UUID;

@ExtendWith(MockitoExtension.class)
class LambdaClientFactoryTest {
    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @Mock
    private AwsCredentialsProvider awsCredentialsProvider;

    @BeforeEach
    void setUp() {
        // No setup needed here as we're mocking static methods in tests
    }

    @Test
    void createLambdaClient_with_real_LambdaClient() {
        try (var mockedStaticLambdaClient = mockStatic(LambdaClient.class)) {
            LambdaClientBuilder lambdaClientBuilder = mock(LambdaClientBuilder.class);
            mockedStaticLambdaClient.when(LambdaClient::builder).thenReturn(lambdaClientBuilder);

            when(lambdaClientBuilder.region(any(Region.class))).thenReturn(lambdaClientBuilder);
            when(lambdaClientBuilder.credentialsProvider(any(AwsCredentialsProvider.class))).thenReturn(lambdaClientBuilder);
            when(lambdaClientBuilder.overrideConfiguration(any(ClientOverrideConfiguration.class))).thenReturn(lambdaClientBuilder);
            when(lambdaClientBuilder.build()).thenReturn(mock(LambdaClient.class));

            when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.US_EAST_1);
            when(awsCredentialsSupplier.getProvider(any(AwsCredentialsOptions.class))).thenReturn(awsCredentialsProvider);

            final LambdaClient lambdaClient = LambdaClientFactory.createLambdaClient(awsAuthenticationOptions, 3, awsCredentialsSupplier);

            assertThat(lambdaClient, notNullValue());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"us-east-1", "us-west-2", "eu-central-1"})
    void createlambdaClient_provides_correct_inputs(final String regionString) {
        try (var mockedStaticLambdaClient = mockStatic(LambdaClient.class)) {
            LambdaClientBuilder lambdaClientBuilder = mock(LambdaClientBuilder.class);
            mockedStaticLambdaClient.when(LambdaClient::builder).thenReturn(lambdaClientBuilder);

            final Region region = Region.of(regionString);
            final String stsRoleArn = UUID.randomUUID().toString();
            final Map<String, String> stsHeaderOverrides = Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
            when(awsAuthenticationOptions.getAwsRegion()).thenReturn(region);
            when(awsAuthenticationOptions.getAwsStsRoleArn()).thenReturn(stsRoleArn);
            when(awsAuthenticationOptions.getAwsStsHeaderOverrides()).thenReturn(stsHeaderOverrides);
            when(awsCredentialsSupplier.getProvider(any())).thenReturn(awsCredentialsProvider);

            when(lambdaClientBuilder.region(any(Region.class))).thenReturn(lambdaClientBuilder);
            when(lambdaClientBuilder.credentialsProvider(any(AwsCredentialsProvider.class))).thenReturn(lambdaClientBuilder);
            when(lambdaClientBuilder.overrideConfiguration(any(ClientOverrideConfiguration.class))).thenReturn(lambdaClientBuilder);
            when(lambdaClientBuilder.build()).thenReturn(mock(LambdaClient.class));

            final LambdaClient lambdaClient = LambdaClientFactory.createLambdaClient(awsAuthenticationOptions, 3, awsCredentialsSupplier);

            final ArgumentCaptor<AwsCredentialsProvider> credentialsProviderArgumentCaptor = ArgumentCaptor.forClass(AwsCredentialsProvider.class);
            verify(lambdaClientBuilder).credentialsProvider(credentialsProviderArgumentCaptor.capture());
            final AwsCredentialsProvider actualCredentialsProvider = credentialsProviderArgumentCaptor.getValue();
            assertThat(actualCredentialsProvider, equalTo(awsCredentialsProvider));

            final ArgumentCaptor<AwsCredentialsOptions> optionsArgumentCaptor = ArgumentCaptor.forClass(AwsCredentialsOptions.class);
            verify(awsCredentialsSupplier).getProvider(optionsArgumentCaptor.capture());

            final AwsCredentialsOptions actualCredentialsOptions = optionsArgumentCaptor.getValue();
            assertThat(actualCredentialsOptions.getRegion(), equalTo(region));
            assertThat(actualCredentialsOptions.getStsRoleArn(), equalTo(stsRoleArn));
            assertThat(actualCredentialsOptions.getStsHeaderOverrides(), equalTo(stsHeaderOverrides));
        }
    }
}
