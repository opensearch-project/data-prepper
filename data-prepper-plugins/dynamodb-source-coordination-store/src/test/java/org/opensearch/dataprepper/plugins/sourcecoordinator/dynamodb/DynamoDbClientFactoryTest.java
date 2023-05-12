/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sourcecoordinator.dynamodb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DynamoDbClientFactoryTest {

    private String region;
    private String stsRoleArn;

    @Mock
    private DynamoDbClient dynamoDbClient;

    @Mock
    private DynamoDbEnhancedClient dynamoDbEnhancedClient;

    @BeforeEach
    void setup() {
        region = "us-east-1";
        stsRoleArn = "arn:aws:iam::123456789012:role/ddb-role";
    }

    @Test
    void provideDynamoDbEnhancedClient_with_null_stsRole_creates_client_with_default_credentials() {

        try (final MockedStatic<Region> regionMockedStatic = mockStatic(Region.class);
             final MockedStatic<DynamoDbClient> dynamoDbClientMockedStatic = mockStatic(DynamoDbClient.class);
             final MockedStatic<DefaultCredentialsProvider> defaultCredentialsProviderMockedStatic = mockStatic(DefaultCredentialsProvider.class);
             final MockedStatic<DynamoDbEnhancedClient> dynamoDbEnhancedClientMockedStatic = mockStatic(DynamoDbEnhancedClient.class)) {
            regionMockedStatic.when(() -> Region.of(region)).thenReturn(Region.US_EAST_1);
            final DynamoDbClientBuilder dynamoDbClientBuilder = mock(DynamoDbClientBuilder.class);
            dynamoDbClientMockedStatic.when(DynamoDbClient::builder).thenReturn(dynamoDbClientBuilder);

            final DefaultCredentialsProvider defaultCredentialsProvider = mock(DefaultCredentialsProvider.class);
            defaultCredentialsProviderMockedStatic.when(DefaultCredentialsProvider::create).thenReturn(defaultCredentialsProvider);
            when(dynamoDbClientBuilder.region(Region.US_EAST_1)).thenReturn(dynamoDbClientBuilder);
            when(dynamoDbClientBuilder.credentialsProvider(defaultCredentialsProvider)).thenReturn(dynamoDbClientBuilder);
            when(dynamoDbClientBuilder.overrideConfiguration(any(ClientOverrideConfiguration.class))).thenReturn(dynamoDbClientBuilder);
            when(dynamoDbClientBuilder.build()).thenReturn(dynamoDbClient);

            final DynamoDbEnhancedClient.Builder builder = mock(DynamoDbEnhancedClient.Builder.class);

            dynamoDbEnhancedClientMockedStatic.when(DynamoDbEnhancedClient::builder).thenReturn(builder);
            when(builder.dynamoDbClient(dynamoDbClient)).thenReturn(builder);
            when(builder.build()).thenReturn(dynamoDbEnhancedClient);

            final DynamoDbEnhancedClient enhancedClient = DynamoDbClientFactory.provideDynamoDbEnhancedClient(region, null);

            assertThat(enhancedClient, equalTo(dynamoDbEnhancedClient));
        }
    }

    @Test
    void provideDynamoDbEnhancedClient_with_stsRole_creates_client_with_sts_credentials() {

        try (final MockedStatic<Region> regionMockedStatic = mockStatic(Region.class);
             final MockedStatic<DynamoDbClient> dynamoDbClientMockedStatic = mockStatic(DynamoDbClient.class);
             final MockedStatic<StsClient> stsClientMockedStatic = mockStatic(StsClient.class);
             final MockedStatic<AssumeRoleRequest> assumeRoleRequestMockedStatic = mockStatic(AssumeRoleRequest.class);
             final MockedStatic<StsAssumeRoleCredentialsProvider> stsAssumeRoleCredentialsProviderMockedStatic = mockStatic(StsAssumeRoleCredentialsProvider.class);
             final MockedStatic<DynamoDbEnhancedClient> dynamoDbEnhancedClientMockedStatic = mockStatic(DynamoDbEnhancedClient.class)) {
            regionMockedStatic.when(() -> Region.of(region)).thenReturn(Region.US_EAST_1);
            final DynamoDbClientBuilder dynamoDbClientBuilder = mock(DynamoDbClientBuilder.class);
            dynamoDbClientMockedStatic.when(DynamoDbClient::builder).thenReturn(dynamoDbClientBuilder);

            final StsClient stsClient = mock(StsClient.class);
            final StsClientBuilder stsClientBuilder = mock(StsClientBuilder.class);
            stsClientMockedStatic.when(StsClient::builder).thenReturn(stsClientBuilder);
            when(stsClientBuilder.region(Region.US_EAST_1)).thenReturn(stsClientBuilder);
            when(stsClientBuilder.overrideConfiguration(any(ClientOverrideConfiguration.class))).thenReturn(stsClientBuilder);
            when(stsClientBuilder.build()).thenReturn(stsClient);

            final AssumeRoleRequest.Builder assumeRoleBuilder = mock(AssumeRoleRequest.Builder.class);
            assumeRoleRequestMockedStatic.when(AssumeRoleRequest::builder).thenReturn(assumeRoleBuilder);
            when(assumeRoleBuilder.roleSessionName(anyString())).thenReturn(assumeRoleBuilder);
            when(assumeRoleBuilder.roleArn(stsRoleArn)).thenReturn(assumeRoleBuilder);

            final AssumeRoleRequest assumeRoleRequest = mock(AssumeRoleRequest.class);
            when(assumeRoleBuilder.build()).thenReturn(assumeRoleRequest);

            final StsAssumeRoleCredentialsProvider awsCredentialsProvider = mock(StsAssumeRoleCredentialsProvider.class);

            final StsAssumeRoleCredentialsProvider.Builder credentialsBuilder = mock(StsAssumeRoleCredentialsProvider.Builder.class);
            stsAssumeRoleCredentialsProviderMockedStatic.when(StsAssumeRoleCredentialsProvider::builder).thenReturn(credentialsBuilder);
            when(credentialsBuilder.stsClient(stsClient)).thenReturn(credentialsBuilder);
            when(credentialsBuilder.refreshRequest(assumeRoleRequest)).thenReturn(credentialsBuilder);
            when(credentialsBuilder.build()).thenReturn(awsCredentialsProvider);

            when(dynamoDbClientBuilder.region(Region.US_EAST_1)).thenReturn(dynamoDbClientBuilder);
            when(dynamoDbClientBuilder.credentialsProvider(awsCredentialsProvider)).thenReturn(dynamoDbClientBuilder);
            when(dynamoDbClientBuilder.overrideConfiguration(any(ClientOverrideConfiguration.class))).thenReturn(dynamoDbClientBuilder);
            when(dynamoDbClientBuilder.build()).thenReturn(dynamoDbClient);

            final DynamoDbEnhancedClient.Builder builder = mock(DynamoDbEnhancedClient.Builder.class);

            dynamoDbEnhancedClientMockedStatic.when(DynamoDbEnhancedClient::builder).thenReturn(builder);
            when(builder.dynamoDbClient(dynamoDbClient)).thenReturn(builder);
            when(builder.build()).thenReturn(dynamoDbEnhancedClient);

            final DynamoDbEnhancedClient enhancedClient = DynamoDbClientFactory.provideDynamoDbEnhancedClient(region, stsRoleArn);

            assertThat(enhancedClient, equalTo(dynamoDbEnhancedClient));
        }
    }
}
