package org.opensearch.dataprepper.plugins.common.opensearch;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.sink.opensearch.ConnectionConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.AwsAuthenticationConfiguration;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.opensearchserverless.OpenSearchServerlessClient;
import software.amazon.awssdk.services.opensearchserverless.OpenSearchServerlessClientBuilder;

import java.util.Collections;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ServerlessNetworkPolicyUpdaterFactoryTest {

    private AwsCredentialsSupplier mockAwsCredentialsSupplier;
    private AwsCredentialsProvider mockAwsCredentialsProvider;
    private ConnectionConfiguration mockConnectionConfiguration;
    private AwsAuthenticationConfiguration mockAwsAuthenticationConfiguration;

    @BeforeEach
    void setUp() {
        // Mock dependencies
        mockAwsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        mockAwsCredentialsProvider = mock(AwsCredentialsProvider.class);
        mockConnectionConfiguration = mock(ConnectionConfiguration.class);
        mockAwsAuthenticationConfiguration = mock(AwsAuthenticationConfiguration.class);
    }

    @Test
    void testCreateWithConnectionConfiguration() {
        try (MockedStatic<OpenSearchServerlessClient> mockedClient = Mockito.mockStatic(OpenSearchServerlessClient.class)) {
            // Mock the OpenSearchServerlessClient builder and its methods
            OpenSearchServerlessClientBuilder builderMock = mock(OpenSearchServerlessClientBuilder.class);
            OpenSearchServerlessClient clientMock = mock(OpenSearchServerlessClient.class);

            mockedClient.when(OpenSearchServerlessClient::builder).thenReturn(builderMock);
            when(builderMock.credentialsProvider(any(AwsCredentialsProvider.class))).thenReturn(builderMock);
            when(builderMock.region(any(Region.class))).thenReturn(builderMock);
            when(builderMock.overrideConfiguration(any(ClientOverrideConfiguration.class))).thenReturn(builderMock);
            when(builderMock.build()).thenReturn(clientMock);

            when(mockAwsCredentialsSupplier.getProvider(any(AwsCredentialsOptions.class))).thenReturn(mockAwsCredentialsProvider);
            when(mockConnectionConfiguration.createAwsCredentialsOptions()).thenReturn(AwsCredentialsOptions.builder()
                .withRegion(Region.AP_EAST_1)
                .withStsRoleArn(UUID.randomUUID().toString())
                .withStsExternalId(UUID.randomUUID().toString())
                .withStsHeaderOverrides(Collections.emptyMap())
                .build());

            // Call the method under test
            ServerlessNetworkPolicyUpdater updater = ServerlessNetworkPolicyUpdaterFactory.create(
                mockAwsCredentialsSupplier,
                mockConnectionConfiguration
            );
        }
    }

    @Test
    void testCreateWithAwsAuthenticationConfiguration() {
        try (MockedStatic<OpenSearchServerlessClient> mockedClient = Mockito.mockStatic(OpenSearchServerlessClient.class)) {
            // Mock the OpenSearchServerlessClient builder and its methods
            OpenSearchServerlessClientBuilder builderMock = mock(OpenSearchServerlessClientBuilder.class);
            OpenSearchServerlessClient clientMock = mock(OpenSearchServerlessClient.class);

            mockedClient.when(OpenSearchServerlessClient::builder).thenReturn(builderMock);
            when(builderMock.credentialsProvider(any(AwsCredentialsProvider.class))).thenReturn(builderMock);
            when(builderMock.region(any(Region.class))).thenReturn(builderMock);
            when(builderMock.overrideConfiguration(any(ClientOverrideConfiguration.class))).thenReturn(builderMock);
            when(builderMock.build()).thenReturn(clientMock);

            when(mockAwsCredentialsSupplier.getProvider(any(AwsCredentialsOptions.class))).thenReturn(mockAwsCredentialsProvider);
            when(mockAwsAuthenticationConfiguration.getAwsRegion()).thenReturn(Region.AF_SOUTH_1);
            when(mockAwsAuthenticationConfiguration.getAwsStsRoleArn()).thenReturn(UUID.randomUUID().toString());
            when(mockAwsAuthenticationConfiguration.getAwsStsExternalId()).thenReturn(UUID.randomUUID().toString());
            when(mockAwsAuthenticationConfiguration.getAwsStsHeaderOverrides()).thenReturn(Collections.emptyMap());

            // Call the method under test
            ServerlessNetworkPolicyUpdater updater = ServerlessNetworkPolicyUpdaterFactory.create(
                mockAwsCredentialsSupplier,
                mockAwsAuthenticationConfiguration
            );

        }
    }
}