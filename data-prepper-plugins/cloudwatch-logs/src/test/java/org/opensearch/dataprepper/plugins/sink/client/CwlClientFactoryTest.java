/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.client;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.sink.config.AwsConfig;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClientBuilder;

import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;

public class CwlClientFactoryTest {
    private AwsConfig awsConfig;
    private AwsCredentialsSupplier awsCredentialsSupplier;
    private AwsCredentialsOptions awsCredentialsOptions;

    @BeforeEach
    void setUp() {
        awsConfig = mock(AwsConfig.class);
        awsCredentialsSupplier = mock(AwsCredentialsSupplier.class);
        awsCredentialsOptions = mock(AwsCredentialsOptions.class);
        when(awsConfig.getAwsRegion()).thenReturn(Region.US_EAST_1);
    }

    @Test
    void check_created_real_default_client_test() {
        final CloudWatchLogsClient cloudWatchLogsClientToTest = CwlClientFactory.createCwlClient(awsConfig, awsCredentialsSupplier);

        assertThat(cloudWatchLogsClientToTest, notNullValue());
    }

    @Test
    void check_created_client_with_no_params() {
        final CloudWatchLogsClient cloudWatchLogsClient = CwlClientFactory.createCwlClient(awsConfig, awsCredentialsSupplier);

        assertThat(cloudWatchLogsClient, notNullValue());
    }

    @Test
    void check_CwlClient_with_correct_inputs() {
        final String stsRoleArn = UUID.randomUUID().toString();
        final String externalId = UUID.randomUUID().toString();
        final Map<String, String> stsHeaderOverrides = Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        when(awsConfig.getAwsStsRoleArn()).thenReturn(stsRoleArn);
        when(awsConfig.getAwsStsExternalId()).thenReturn(externalId);
        when(awsConfig.getAwsStsHeaderOverrides()).thenReturn(stsHeaderOverrides);

        final AwsCredentialsProvider expectedCredentialsProvider = mock(AwsCredentialsProvider.class);
        when(awsCredentialsSupplier.getProvider(ArgumentMatchers.any())).thenReturn(expectedCredentialsProvider);

        final CloudWatchLogsClientBuilder cloudWatchLogsClientBuilder = mock(CloudWatchLogsClientBuilder.class);
        when(cloudWatchLogsClientBuilder.region(awsConfig.getAwsRegion())).thenReturn(cloudWatchLogsClientBuilder);
        when(cloudWatchLogsClientBuilder.credentialsProvider(ArgumentMatchers.any())).thenReturn(cloudWatchLogsClientBuilder);
        when(cloudWatchLogsClientBuilder.overrideConfiguration(ArgumentMatchers.any(ClientOverrideConfiguration.class))).thenReturn(cloudWatchLogsClientBuilder);
        try(final MockedStatic<CloudWatchLogsClient> cloudWatchLogsClientMockedStatic = mockStatic(CloudWatchLogsClient.class)) {
            cloudWatchLogsClientMockedStatic.when(CloudWatchLogsClient::builder)
                    .thenReturn(cloudWatchLogsClientBuilder);
            CwlClientFactory.createCwlClient(awsConfig, awsCredentialsSupplier);
        }

        final ArgumentCaptor<AwsCredentialsProvider> credentialsProviderArgumentCaptor = ArgumentCaptor.forClass(AwsCredentialsProvider.class);
        verify(cloudWatchLogsClientBuilder).credentialsProvider(credentialsProviderArgumentCaptor.capture());

        final AwsCredentialsProvider actualProvider = credentialsProviderArgumentCaptor.getValue();

        assertThat(actualProvider, equalTo(expectedCredentialsProvider));

        final ArgumentCaptor<AwsCredentialsOptions> credentialsOptionsArgumentCaptor = ArgumentCaptor.forClass(AwsCredentialsOptions.class);
        verify(awsCredentialsSupplier).getProvider(credentialsOptionsArgumentCaptor.capture());

        final AwsCredentialsOptions actualOptions = credentialsOptionsArgumentCaptor.getValue();
        assertThat(actualOptions.getRegion(), equalTo(awsConfig.getAwsRegion()));
        assertThat(actualOptions.getStsRoleArn(), equalTo(awsConfig.getAwsStsRoleArn()));
        assertThat(actualOptions.getStsExternalId(), equalTo(awsConfig.getAwsStsExternalId()));
        assertThat(actualOptions.getStsHeaderOverrides(), equalTo(awsConfig.getAwsStsHeaderOverrides()));
    }
}
