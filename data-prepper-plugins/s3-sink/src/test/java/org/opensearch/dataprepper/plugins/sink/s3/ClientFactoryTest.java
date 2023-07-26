/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.s3;

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
import org.opensearch.dataprepper.plugins.sink.s3.configuration.AwsAuthenticationOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

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
class ClientFactoryTest {
    @Mock
    private S3SinkConfig s3SinkConfig;
    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @BeforeEach
    void setUp() {
        when(s3SinkConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
    }

    @Test
    void createS3Client_with_real_S3Client() {
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.US_EAST_1);
        final S3Client s3Client = ClientFactory.createS3Client(s3SinkConfig, awsCredentialsSupplier);

        assertThat(s3Client, notNullValue());
    }

    @ParameterizedTest
    @ValueSource(strings = {"us-east-1", "us-west-2", "eu-central-1"})
    void createS3Client_provides_correct_inputs(final String regionString) {
        final Region region = Region.of(regionString);
        final String stsRoleArn = UUID.randomUUID().toString();
        final String externalId = UUID.randomUUID().toString();
        final Map<String, String> stsHeaderOverrides = Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(region);
        when(awsAuthenticationOptions.getAwsStsRoleArn()).thenReturn(stsRoleArn);
        when(awsAuthenticationOptions.getAwsStsExternalId()).thenReturn(externalId);
        when(awsAuthenticationOptions.getAwsStsHeaderOverrides()).thenReturn(stsHeaderOverrides);

        final AwsCredentialsProvider expectedCredentialsProvider = mock(AwsCredentialsProvider.class);
        when(awsCredentialsSupplier.getProvider(any())).thenReturn(expectedCredentialsProvider);

        final S3ClientBuilder s3ClientBuilder = mock(S3ClientBuilder.class);
        when(s3ClientBuilder.region(region)).thenReturn(s3ClientBuilder);
        when(s3ClientBuilder.credentialsProvider(any())).thenReturn(s3ClientBuilder);
        when(s3ClientBuilder.overrideConfiguration(any(ClientOverrideConfiguration.class))).thenReturn(s3ClientBuilder);
        try(final MockedStatic<S3Client> s3ClientMockedStatic = mockStatic(S3Client.class)) {
            s3ClientMockedStatic.when(S3Client::builder)
                    .thenReturn(s3ClientBuilder);
            ClientFactory.createS3Client(s3SinkConfig, awsCredentialsSupplier);
        }

        final ArgumentCaptor<AwsCredentialsProvider> credentialsProviderArgumentCaptor = ArgumentCaptor.forClass(AwsCredentialsProvider.class);
        verify(s3ClientBuilder).credentialsProvider(credentialsProviderArgumentCaptor.capture());

        final AwsCredentialsProvider actualCredentialsProvider = credentialsProviderArgumentCaptor.getValue();

        assertThat(actualCredentialsProvider, equalTo(expectedCredentialsProvider));

        final ArgumentCaptor<AwsCredentialsOptions> optionsArgumentCaptor = ArgumentCaptor.forClass(AwsCredentialsOptions.class);
        verify(awsCredentialsSupplier).getProvider(optionsArgumentCaptor.capture());

        final AwsCredentialsOptions actualCredentialsOptions = optionsArgumentCaptor.getValue();
        assertThat(actualCredentialsOptions.getRegion(), equalTo(region));
        assertThat(actualCredentialsOptions.getStsRoleArn(), equalTo(stsRoleArn));
        assertThat(actualCredentialsOptions.getStsExternalId(), equalTo(externalId));
        assertThat(actualCredentialsOptions.getStsHeaderOverrides(), equalTo(stsHeaderOverrides));
    }
}