/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.plugins.source.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.source.configuration.SqsOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class SqsServiceTest {

    @Mock
    private SqsClient sqsClient;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private S3SourceConfig s3SourceConfig;

    @Mock
    private S3Service s3Service;

    @Mock
    private AwsCredentialsProvider awsCredentialsProvider;

    @Mock
    private PluginMetrics pluginMetrics;

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void sqsService_does_not_set_visibility_timeout_from_queue_attributes_when_visibility_timeout_is_set(final boolean acknowledgmentsEnabled) {
        when(s3SourceConfig.getAcknowledgements()).thenReturn(acknowledgmentsEnabled);

        final SqsOptions sqsOptions = mock(SqsOptions.class);
        lenient().when(sqsOptions.getVisibilityTimeout()).thenReturn(Duration.ofSeconds(100));
        lenient().when(s3SourceConfig.getSqsOptions()).thenReturn(sqsOptions);

        final SqsClientBuilder sqsClientBuilder = mock(SqsClientBuilder.class);
        final AwsAuthenticationOptions awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.US_EAST_1);
        when(s3SourceConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);

        when(sqsClientBuilder.region(Region.US_EAST_1)).thenReturn(sqsClientBuilder);
        when(sqsClientBuilder.credentialsProvider(awsCredentialsProvider)).thenReturn(sqsClientBuilder);
        when(sqsClientBuilder.overrideConfiguration(any(ClientOverrideConfiguration.class))).thenReturn(sqsClientBuilder);
        when(sqsClientBuilder.build()).thenReturn(sqsClient);
        try (final MockedStatic<SqsClient> sqsClientMockedStatic = mockStatic(SqsClient.class)) {
            sqsClientMockedStatic.when(SqsClient::builder).thenReturn(sqsClientBuilder);

            final SqsService objectUnderTest = new SqsService(acknowledgementSetManager, s3SourceConfig, s3Service, pluginMetrics, awsCredentialsProvider);
        }

        verify(sqsClient, never()).getQueueAttributes(any(GetQueueAttributesRequest.class));

    }

    @Test
    void sqsService_sets_visibility_timeout_from_queue_attributes_when_visibility_timeout_is_not_set_and_acknowledgments_enabled() {
        when(s3SourceConfig.getAcknowledgements()).thenReturn(true);

        final SqsOptions sqsOptions = mock(SqsOptions.class);
        when(sqsOptions.getSqsUrl()).thenReturn(UUID.randomUUID().toString());
        when(sqsOptions.getVisibilityTimeout()).thenReturn(null);
        when(s3SourceConfig.getSqsOptions()).thenReturn(sqsOptions);


        final SqsClientBuilder sqsClientBuilder = mock(SqsClientBuilder.class);
        final AwsAuthenticationOptions awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.US_EAST_1);
        when(s3SourceConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);

        when(sqsClientBuilder.region(Region.US_EAST_1)).thenReturn(sqsClientBuilder);
        when(sqsClientBuilder.credentialsProvider(awsCredentialsProvider)).thenReturn(sqsClientBuilder);
        when(sqsClientBuilder.overrideConfiguration(any(ClientOverrideConfiguration.class))).thenReturn(sqsClientBuilder);
        when(sqsClientBuilder.build()).thenReturn(sqsClient);

        final ArgumentCaptor<GetQueueAttributesRequest> argumentCaptor = ArgumentCaptor.forClass(GetQueueAttributesRequest.class);

        final GetQueueAttributesResponse getQueueAttributesResponse = mock(GetQueueAttributesResponse.class);
        when(getQueueAttributesResponse.attributes()).thenReturn(Map.of(QueueAttributeName.VISIBILITY_TIMEOUT, "1000"));
        when(sqsClient.getQueueAttributes(argumentCaptor.capture())).thenReturn(getQueueAttributesResponse);
        try (final MockedStatic<SqsClient> sqsClientMockedStatic = mockStatic(SqsClient.class)) {
            sqsClientMockedStatic.when(SqsClient::builder).thenReturn(sqsClientBuilder);

            final SqsService objectUnderTest = new SqsService(acknowledgementSetManager, s3SourceConfig, s3Service, pluginMetrics, awsCredentialsProvider);
        }

        final GetQueueAttributesRequest getQueueAttributesRequest = argumentCaptor.getValue();
        assertThat(getQueueAttributesRequest, notNullValue());
        assertThat(getQueueAttributesRequest.queueUrl(), equalTo(sqsOptions.getSqsUrl()));
        assertThat(getQueueAttributesRequest.attributeNames(), equalTo(List.of(QueueAttributeName.VISIBILITY_TIMEOUT)));

        verify(sqsOptions).setVisibilityTimeout(Duration.ofSeconds(1000));

    }
}
