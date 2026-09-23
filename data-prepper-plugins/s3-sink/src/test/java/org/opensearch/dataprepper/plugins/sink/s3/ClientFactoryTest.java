/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
import org.opensearch.dataprepper.plugins.sink.s3.configuration.ClientOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.SdkPlugin;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.LegacyMd5Plugin;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
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
    @Mock
    private ClientOptions clientOptions;

    @BeforeEach
    void setUp() {
        when(s3SinkConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
    }

    @Test
    void createS3AsyncClient_with_real_S3AsyncClient() {
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.US_EAST_1);
        final S3AsyncClient s3Client = ClientFactory.createS3AsyncClient(s3SinkConfig, awsCredentialsSupplier);

        assertThat(s3Client, notNullValue());
    }

    @ParameterizedTest
    @ValueSource(strings = {"us-east-1", "us-west-2", "eu-central-1"})
    void createS3AsyncClient_with_client_options_returns_expected_client(final String regionString) {
        final Region region = Region.of(regionString);
        final String stsRoleArn = UUID.randomUUID().toString();
        final String externalId = UUID.randomUUID().toString();
        final Map<String, String> stsHeaderOverrides = Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(region);
        when(awsAuthenticationOptions.getAwsStsRoleArn()).thenReturn(stsRoleArn);
        when(awsAuthenticationOptions.getAwsStsExternalId()).thenReturn(externalId);
        when(awsAuthenticationOptions.getAwsStsHeaderOverrides()).thenReturn(stsHeaderOverrides);

        final AwsCredentialsProvider expectedCredentialsProvider = mock(AwsCredentialsProvider.class);
        when(awsCredentialsSupplier.getProvider(any(AwsCredentialsOptions.class))).thenReturn(expectedCredentialsProvider);

        final S3AsyncClientBuilder s3AsyncClientBuilder = mock(S3AsyncClientBuilder.class);
        when(s3AsyncClientBuilder.region(region)).thenReturn(s3AsyncClientBuilder);
        when(s3AsyncClientBuilder.crossRegionAccessEnabled(true)).thenReturn(s3AsyncClientBuilder);
        when(s3AsyncClientBuilder.forcePathStyle(anyBoolean())).thenReturn(s3AsyncClientBuilder);
        when(s3AsyncClientBuilder.credentialsProvider(any())).thenReturn(s3AsyncClientBuilder);
        when(s3AsyncClientBuilder.overrideConfiguration(any(ClientOverrideConfiguration.class))).thenReturn(s3AsyncClientBuilder);

        when(s3SinkConfig.getClientOptions()).thenReturn(clientOptions);

        final int maxConnections = 100;
        final Duration acquireTimeout = Duration.ofSeconds(30);
        when(clientOptions.getMaxConnections()).thenReturn(maxConnections);
        when(clientOptions.getAcquireTimeout()).thenReturn(acquireTimeout);

        final NettyNioAsyncHttpClient.Builder httpClientBuilder = mock(NettyNioAsyncHttpClient.Builder.class);
        final SdkAsyncHttpClient httpClient = mock(SdkAsyncHttpClient.class);
        when(httpClientBuilder.connectionAcquisitionTimeout(any(Duration.class))).thenReturn(httpClientBuilder);
        when(httpClientBuilder.maxConcurrency(anyInt())).thenReturn(httpClientBuilder);
        when(httpClientBuilder.build()).thenReturn(httpClient);

        try(final MockedStatic<S3AsyncClient> s3AsyncClientMockedStatic = mockStatic(S3AsyncClient.class);
            final MockedStatic<NettyNioAsyncHttpClient> httpClientMockedStatic = mockStatic(NettyNioAsyncHttpClient.class)) {
            s3AsyncClientMockedStatic.when(S3AsyncClient::builder)
                    .thenReturn(s3AsyncClientBuilder);
            httpClientMockedStatic.when(NettyNioAsyncHttpClient::builder)
                    .thenReturn(httpClientBuilder);
            ClientFactory.createS3AsyncClient(s3SinkConfig, awsCredentialsSupplier);
        }

        final ArgumentCaptor<AwsCredentialsProvider> credentialsProviderArgumentCaptor = ArgumentCaptor.forClass(AwsCredentialsProvider.class);
        verify(s3AsyncClientBuilder).credentialsProvider(credentialsProviderArgumentCaptor.capture());

        final AwsCredentialsProvider actualCredentialsProvider = credentialsProviderArgumentCaptor.getValue();

        assertThat(actualCredentialsProvider, equalTo(expectedCredentialsProvider));

        final ArgumentCaptor<AwsCredentialsOptions> optionsArgumentCaptor = ArgumentCaptor.forClass(AwsCredentialsOptions.class);
        verify(awsCredentialsSupplier).getProvider(optionsArgumentCaptor.capture());

        final AwsCredentialsOptions actualCredentialsOptions = optionsArgumentCaptor.getValue();
        assertThat(actualCredentialsOptions.getRegion(), equalTo(region));
        assertThat(actualCredentialsOptions.getStsRoleArn(), equalTo(stsRoleArn));
        assertThat(actualCredentialsOptions.getStsExternalId(), equalTo(externalId));
        assertThat(actualCredentialsOptions.getStsHeaderOverrides(), equalTo(stsHeaderOverrides));

        verify(httpClientBuilder).connectionAcquisitionTimeout(acquireTimeout);
        verify(httpClientBuilder).maxConcurrency(maxConnections);
        verify(s3AsyncClientBuilder).httpClient(httpClient);
    }

    @Test
    void createS3AsyncClient_with_endpoint_overrides_endpoint_and_disables_cross_region_access() {
        final String endpoint = "http://localhost:9000";
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.US_EAST_1);
        when(s3SinkConfig.getEndpoint()).thenReturn(endpoint);
        when(s3SinkConfig.getForcePathStyle()).thenReturn(true);

        final S3AsyncClientBuilder s3AsyncClientBuilder = mock(S3AsyncClientBuilder.class);
        when(s3AsyncClientBuilder.region(any(Region.class))).thenReturn(s3AsyncClientBuilder);
        when(s3AsyncClientBuilder.crossRegionAccessEnabled(anyBoolean())).thenReturn(s3AsyncClientBuilder);
        when(s3AsyncClientBuilder.forcePathStyle(anyBoolean())).thenReturn(s3AsyncClientBuilder);
        when(s3AsyncClientBuilder.credentialsProvider(any())).thenReturn(s3AsyncClientBuilder);
        when(s3AsyncClientBuilder.overrideConfiguration(any(ClientOverrideConfiguration.class))).thenReturn(s3AsyncClientBuilder);

        try (final MockedStatic<S3AsyncClient> s3AsyncClientMockedStatic = mockStatic(S3AsyncClient.class)) {
            s3AsyncClientMockedStatic.when(S3AsyncClient::builder).thenReturn(s3AsyncClientBuilder);
            ClientFactory.createS3AsyncClient(s3SinkConfig, awsCredentialsSupplier);
        }

        final ArgumentCaptor<URI> endpointCaptor = ArgumentCaptor.forClass(URI.class);
        verify(s3AsyncClientBuilder).endpointOverride(endpointCaptor.capture());
        assertThat(endpointCaptor.getValue(), equalTo(URI.create(endpoint)));
        verify(s3AsyncClientBuilder).forcePathStyle(true);
        verify(s3AsyncClientBuilder).crossRegionAccessEnabled(false);
    }

    @Test
    void createS3AsyncClient_without_endpoint_does_not_override_endpoint() {
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.US_EAST_1);

        final S3AsyncClientBuilder s3AsyncClientBuilder = mock(S3AsyncClientBuilder.class);
        when(s3AsyncClientBuilder.region(any(Region.class))).thenReturn(s3AsyncClientBuilder);
        when(s3AsyncClientBuilder.crossRegionAccessEnabled(anyBoolean())).thenReturn(s3AsyncClientBuilder);
        when(s3AsyncClientBuilder.forcePathStyle(anyBoolean())).thenReturn(s3AsyncClientBuilder);
        when(s3AsyncClientBuilder.credentialsProvider(any())).thenReturn(s3AsyncClientBuilder);
        when(s3AsyncClientBuilder.overrideConfiguration(any(ClientOverrideConfiguration.class))).thenReturn(s3AsyncClientBuilder);

        try (final MockedStatic<S3AsyncClient> s3AsyncClientMockedStatic = mockStatic(S3AsyncClient.class)) {
            s3AsyncClientMockedStatic.when(S3AsyncClient::builder).thenReturn(s3AsyncClientBuilder);
            ClientFactory.createS3AsyncClient(s3SinkConfig, awsCredentialsSupplier);
        }

        verify(s3AsyncClientBuilder, never()).endpointOverride(any(URI.class));
        verify(s3AsyncClientBuilder).crossRegionAccessEnabled(true);
        verify(s3AsyncClientBuilder).forcePathStyle(false);
        verify(s3AsyncClientBuilder, never()).addPlugin(any());
    }

    @Test
    void createS3AsyncClient_with_legacy_md5_checksum_configures_checksum_behavior() {
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.US_EAST_1);
        when(s3SinkConfig.getLegacyMd5Checksum()).thenReturn(true);

        final S3AsyncClientBuilder s3AsyncClientBuilder = mock(S3AsyncClientBuilder.class);
        when(s3AsyncClientBuilder.region(any(Region.class))).thenReturn(s3AsyncClientBuilder);
        when(s3AsyncClientBuilder.crossRegionAccessEnabled(anyBoolean())).thenReturn(s3AsyncClientBuilder);
        when(s3AsyncClientBuilder.forcePathStyle(anyBoolean())).thenReturn(s3AsyncClientBuilder);
        when(s3AsyncClientBuilder.credentialsProvider(any())).thenReturn(s3AsyncClientBuilder);
        when(s3AsyncClientBuilder.overrideConfiguration(any(ClientOverrideConfiguration.class))).thenReturn(s3AsyncClientBuilder);
        when(s3AsyncClientBuilder.requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)).thenReturn(s3AsyncClientBuilder);
        when(s3AsyncClientBuilder.responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED)).thenReturn(s3AsyncClientBuilder);

        try (final MockedStatic<S3AsyncClient> s3AsyncClientMockedStatic = mockStatic(S3AsyncClient.class)) {
            s3AsyncClientMockedStatic.when(S3AsyncClient::builder).thenReturn(s3AsyncClientBuilder);
            ClientFactory.createS3AsyncClient(s3SinkConfig, awsCredentialsSupplier);
        }

        verify(s3AsyncClientBuilder).requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED);
        verify(s3AsyncClientBuilder).responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED);

        final ArgumentCaptor<SdkPlugin> pluginCaptor = ArgumentCaptor.forClass(SdkPlugin.class);
        verify(s3AsyncClientBuilder).addPlugin(pluginCaptor.capture());
        assertThat(pluginCaptor.getValue(), instanceOf(LegacyMd5Plugin.class));
    }
}