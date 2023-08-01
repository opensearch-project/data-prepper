/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.sns;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.sink.sns.configuration.AwsAuthenticationOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.SnsClientBuilder;

import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SnsClientFactoryTest {
    @Mock
    private SnsSinkConfig snsSinkConfig;
    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @BeforeEach
    void setUp() {
        when(snsSinkConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
    }

    @Test
    void createSnsClient_with_real_SnsClient() {
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.US_EAST_1);
        final SnsClient s3Client = SnsClientFactory.createSNSClient(snsSinkConfig, awsCredentialsSupplier);

        assertThat(s3Client, notNullValue());
    }

    @ParameterizedTest
    @ValueSource(strings = {"us-east-1", "us-west-2", "eu-central-1"})
    void createSNSClient_provides_correct_inputs(final String regionString) {
        final Region region = Region.of(regionString);
        final String stsRoleArn = UUID.randomUUID().toString();
        final Map<String, String> stsHeaderOverrides = Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(region);
        when(awsAuthenticationOptions.getAwsStsRoleArn()).thenReturn(stsRoleArn);
        when(awsAuthenticationOptions.getAwsStsHeaderOverrides()).thenReturn(stsHeaderOverrides);

        final AwsCredentialsProvider expectedCredentialsProvider = mock(AwsCredentialsProvider.class);
        when(awsCredentialsSupplier.getProvider(any())).thenReturn(expectedCredentialsProvider);

        final SnsClientBuilder snsClientBuilder = mock(SnsClientBuilder.class);
        when(snsClientBuilder.region(region)).thenReturn(snsClientBuilder);
        when(snsClientBuilder.credentialsProvider(any())).thenReturn(snsClientBuilder);
        when(snsClientBuilder.overrideConfiguration(any(ClientOverrideConfiguration.class))).thenReturn(snsClientBuilder);
        try(final MockedStatic<SnsClient> s3ClientMockedStatic = mockStatic(SnsClient.class)) {
            s3ClientMockedStatic.when(SnsClient::builder)
                    .thenReturn(snsClientBuilder);
            SnsClientFactory.createSNSClient(snsSinkConfig, awsCredentialsSupplier);
        }

        final ArgumentCaptor<AwsCredentialsProvider> credentialsProviderArgumentCaptor = ArgumentCaptor.forClass(AwsCredentialsProvider.class);
        verify(snsClientBuilder).credentialsProvider(credentialsProviderArgumentCaptor.capture());

        final AwsCredentialsProvider actualCredentialsProvider = credentialsProviderArgumentCaptor.getValue();

        assertThat(actualCredentialsProvider, equalTo(expectedCredentialsProvider));

        final ArgumentCaptor<AwsCredentialsOptions> optionsArgumentCaptor = ArgumentCaptor.forClass(AwsCredentialsOptions.class);
        verify(awsCredentialsSupplier).getProvider(optionsArgumentCaptor.capture());

        final AwsCredentialsOptions actualCredentialsOptions = optionsArgumentCaptor.getValue();
        assertThat(actualCredentialsOptions.getRegion(), equalTo(region));
        assertThat(actualCredentialsOptions.getStsRoleArn(), equalTo(stsRoleArn));
        assertThat(actualCredentialsOptions.getStsHeaderOverrides(), equalTo(stsHeaderOverrides));
    }
}