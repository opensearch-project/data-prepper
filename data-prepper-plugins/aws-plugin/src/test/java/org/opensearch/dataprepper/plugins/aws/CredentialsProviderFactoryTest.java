/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.aws;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.backoff.EqualJitterBackoffStrategy;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CredentialsProviderFactoryTest {
    public static final int MAXIMUM_ROLE_SESSION_LENGTH = 64;
    @Mock
    private AwsCredentialsOptions awsCredentialsOptions;

    @Mock
    private AwsStsConfiguration defaultStsConfiguration;

    private CredentialsProviderFactory createObjectUnderTest() {
        return new CredentialsProviderFactory(defaultStsConfiguration);
    }

    @Test
    void providerFromOptions_with_null_AwsCredentialsOptions_throws() {
        final CredentialsProviderFactory objectUnderTest = createObjectUnderTest();
        assertThrows(NullPointerException.class, () -> objectUnderTest.providerFromOptions(null));
    }

    @Test
    void providerFromOptions_without_StsRoleArn_returns_DefaultCredentialsProvider() {
        assertThat(createObjectUnderTest().providerFromOptions(awsCredentialsOptions),
                instanceOf(DefaultCredentialsProvider.class));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "notanarn:something:else",
            "arn:aws:notiam::123456789012:role/TestRole",
            "arn:aws:iam::123456789012:notrole/TestRole",
            "arn:aws:iam::123456789012:/",
            "arn:aws:iam::123456789012:"
    })
    void providerFromOptions_with_invalid_StsRoleArn_throws(final String stsRoleArn) {
        when(awsCredentialsOptions.getStsRoleArn()).thenReturn(stsRoleArn);
        final CredentialsProviderFactory objectUnderTest = createObjectUnderTest();

        assertThrows(IllegalArgumentException.class, () -> objectUnderTest.providerFromOptions(awsCredentialsOptions));
    }

    @Test
    void providerFromOptions_with_StsRoleArn() {
        when(awsCredentialsOptions.getStsRoleArn())
                .thenReturn(createStsRole());
        when(awsCredentialsOptions.getRegion())
                .thenReturn(Region.US_EAST_1);
        final AwsCredentialsProvider awsCredentialsProvider = createObjectUnderTest().providerFromOptions(awsCredentialsOptions);
        assertThat(awsCredentialsProvider, instanceOf(StsAssumeRoleCredentialsProvider.class));
    }

    @ParameterizedTest
    @MethodSource("getRegions")
    void getDefaultRegion_returns_expected_region(final Region region) {
        when(defaultStsConfiguration.getAwsRegion()).thenReturn(region);

        final CredentialsProviderFactory credentialsProviderFactory = createObjectUnderTest();

        final Region actualRegion = credentialsProviderFactory.getDefaultRegion();

        assertThat(actualRegion, equalTo(region));
    }

    private static List<Region> getRegions() {
        return Region.regions();
    }


    @Nested
    class WithSts {
        @Mock
        private StsClient stsClient;
        @Mock
        private StsClientBuilder stsClientBuilder;
        @Mock
        private StsAssumeRoleCredentialsProvider stsCredentialsProvider;
        @Mock(lenient = true)
        private StsAssumeRoleCredentialsProvider.Builder stsCredentialsProviderBuilder;
        private String testStsRole;

        @BeforeEach
        void setUp() {
            when(stsClientBuilder.overrideConfiguration(any(ClientOverrideConfiguration.class))).thenReturn(stsClientBuilder);
            when(stsClientBuilder.build()).thenReturn(stsClient);

            when(stsCredentialsProviderBuilder.stsClient(stsClient)).thenReturn(stsCredentialsProviderBuilder);
            when(stsCredentialsProviderBuilder.refreshRequest(any(AssumeRoleRequest.class))).thenReturn(stsCredentialsProviderBuilder);
            when(stsCredentialsProviderBuilder.build()).thenReturn(stsCredentialsProvider);

            testStsRole = createStsRole();
        }

        @ParameterizedTest
        @ValueSource(strings = {"us-east-1", "us-west-2", "eu-west-1"})
        void providerFromOptions_should_return_StsCredentialsProvider_with_sts_role_arn(final String regionString) {
            final String externalId = UUID.randomUUID().toString();
            final Region region = Region.of(regionString);
            when(awsCredentialsOptions.getRegion()).thenReturn(region);
            when(awsCredentialsOptions.getStsRoleArn()).thenReturn(testStsRole);
            when(awsCredentialsOptions.getStsExternalId()).thenReturn(externalId);

            when(stsClientBuilder.region(region)).thenReturn(stsClientBuilder);

            final CredentialsProviderFactory objectUnderTest = createObjectUnderTest();
            final AwsCredentialsProvider actualCredentialsProvider;
            try (final MockedStatic<StsClient> stsClientMockedStatic = mockStatic(StsClient.class);
                 final MockedStatic<StsAssumeRoleCredentialsProvider> credentialsProviderMockedStatic = mockStatic(StsAssumeRoleCredentialsProvider.class)) {
                stsClientMockedStatic.when(StsClient::builder).thenReturn(stsClientBuilder);
                credentialsProviderMockedStatic.when(StsAssumeRoleCredentialsProvider::builder).thenReturn(stsCredentialsProviderBuilder);
                actualCredentialsProvider = objectUnderTest.providerFromOptions(awsCredentialsOptions);
            }

            assertThat(actualCredentialsProvider, instanceOf(StsAssumeRoleCredentialsProvider.class));
            assertThat(actualCredentialsProvider, equalTo(stsCredentialsProvider));

            verify(stsClientBuilder).region(region);
            verify(stsClientBuilder).overrideConfiguration(any(ClientOverrideConfiguration.class));
            verify(stsClientBuilder).build();
            final ArgumentCaptor<AssumeRoleRequest> assumeRoleRequestArgumentCaptor = ArgumentCaptor.forClass(AssumeRoleRequest.class);
            verify(stsCredentialsProviderBuilder).refreshRequest(assumeRoleRequestArgumentCaptor.capture());
            verify(stsCredentialsProviderBuilder).stsClient(stsClient);
            verify(stsCredentialsProviderBuilder).build();
            verifyNoMoreInteractions(stsClientBuilder);
            verifyNoMoreInteractions(stsCredentialsProviderBuilder);

            final AssumeRoleRequest assumeRoleRequest = assumeRoleRequestArgumentCaptor.getValue();
            assertThat(assumeRoleRequest.roleArn(), equalTo(testStsRole));
            assertThat(assumeRoleRequest.externalId(), equalTo(externalId));
            assertThat(assumeRoleRequest.roleSessionName(), startsWith("Data-Prepper"));
            assertThat(assumeRoleRequest.roleSessionName().length(), lessThanOrEqualTo(MAXIMUM_ROLE_SESSION_LENGTH));
        }

        @Test
        void providerFromOptions_should_return_s3Client_with_sts_role_arn_when_no_region() {
            when(awsCredentialsOptions.getRegion()).thenReturn(null);
            when(awsCredentialsOptions.getStsRoleArn()).thenReturn(testStsRole);

            final CredentialsProviderFactory objectUnderTest = createObjectUnderTest();
            final AwsCredentialsProvider actualCredentialsProvider;
            try (final MockedStatic<StsClient> stsClientMockedStatic = mockStatic(StsClient.class)) {
                stsClientMockedStatic.when(StsClient::builder).thenReturn(stsClientBuilder);
                actualCredentialsProvider = objectUnderTest.providerFromOptions(awsCredentialsOptions);
            }

            assertThat(actualCredentialsProvider, instanceOf(StsAssumeRoleCredentialsProvider.class));

            verify(stsClientBuilder, never()).region(any(Region.class));
        }

        @Test
        void providerFromOptions_should_override_STS_Headers_when_HeaderOverrides_when_set() {
            final String headerName1 = UUID.randomUUID().toString();
            final String headerValue1 = UUID.randomUUID().toString();
            final String headerName2 = UUID.randomUUID().toString();
            final String headerValue2 = UUID.randomUUID().toString();
            final Map<String, String> overrideHeaders = Map.of(headerName1, headerValue1, headerName2, headerValue2);

            when(awsCredentialsOptions.getRegion()).thenReturn(Region.US_EAST_1);
            when(awsCredentialsOptions.getStsRoleArn()).thenReturn(testStsRole);
            when(awsCredentialsOptions.getStsHeaderOverrides()).thenReturn(overrideHeaders);

            when(stsClientBuilder.region(Region.US_EAST_1)).thenReturn(stsClientBuilder);

            final CredentialsProviderFactory objectUnderTest = createObjectUnderTest();
            final AwsCredentialsProvider actualCredentialsProvider;
            try (final MockedStatic<StsClient> stsClientMockedStatic = mockStatic(StsClient.class);
                 final MockedStatic<StsAssumeRoleCredentialsProvider> credentialsProviderMockedStatic = mockStatic(StsAssumeRoleCredentialsProvider.class)) {
                stsClientMockedStatic.when(StsClient::builder).thenReturn(stsClientBuilder);
                credentialsProviderMockedStatic.when(StsAssumeRoleCredentialsProvider::builder).thenReturn(stsCredentialsProviderBuilder);
                actualCredentialsProvider = objectUnderTest.providerFromOptions(awsCredentialsOptions);
            }

            assertThat(actualCredentialsProvider, instanceOf(StsAssumeRoleCredentialsProvider.class));

            final ArgumentCaptor<AssumeRoleRequest> assumeRoleRequestArgumentCaptor = ArgumentCaptor.forClass(AssumeRoleRequest.class);
            verify(stsCredentialsProviderBuilder).refreshRequest(assumeRoleRequestArgumentCaptor.capture());

            final AssumeRoleRequest actualAssumeRoleRequest = assumeRoleRequestArgumentCaptor.getValue();
            assertThat(actualAssumeRoleRequest.roleArn(), equalTo(testStsRole));
            assertThat(actualAssumeRoleRequest.roleSessionName(), startsWith("Data-Prepper-"));
            assertThat(actualAssumeRoleRequest.roleSessionName().length(), lessThanOrEqualTo(MAXIMUM_ROLE_SESSION_LENGTH));
            assertThat(actualAssumeRoleRequest.overrideConfiguration(), notNullValue());
            assertThat(actualAssumeRoleRequest.overrideConfiguration().isPresent(), equalTo(true));
            final AwsRequestOverrideConfiguration overrideConfiguration = actualAssumeRoleRequest.overrideConfiguration().get();
            assertThat(overrideConfiguration.headers(), notNullValue());
            assertThat(overrideConfiguration.headers().size(), equalTo(2));
            assertThat(overrideConfiguration.headers(), hasKey(headerName1));
            assertThat(overrideConfiguration.headers(), hasKey(headerName2));
            assertThat(overrideConfiguration.headers().get(headerName1), notNullValue());
            assertThat(overrideConfiguration.headers().get(headerName1).size(), equalTo(1));
            assertThat(overrideConfiguration.headers().get(headerName1), hasItem(headerValue1));
            assertThat(overrideConfiguration.headers().get(headerName2), notNullValue());
            assertThat(overrideConfiguration.headers().get(headerName2).size(), equalTo(1));
            assertThat(overrideConfiguration.headers().get(headerName2), hasItem(headerValue2));
        }

        @Test
        void providerFromOptions_should_not_override_STS_Headers_when_HeaderOverrides_are_empty() {
            when(awsCredentialsOptions.getRegion()).thenReturn(Region.US_EAST_1);
            when(awsCredentialsOptions.getStsRoleArn()).thenReturn(testStsRole);
            when(awsCredentialsOptions.getStsHeaderOverrides()).thenReturn(Collections.emptyMap());

            when(stsClientBuilder.region(Region.US_EAST_1)).thenReturn(stsClientBuilder);

            final CredentialsProviderFactory objectUnderTest = createObjectUnderTest();
            final AwsCredentialsProvider actualCredentialsProvider;

            try (final MockedStatic<StsClient> stsClientMockedStatic = mockStatic(StsClient.class);
                 final MockedStatic<StsAssumeRoleCredentialsProvider> credentialsProviderMockedStatic = mockStatic(StsAssumeRoleCredentialsProvider.class)) {
                stsClientMockedStatic.when(StsClient::builder).thenReturn(stsClientBuilder);
                credentialsProviderMockedStatic.when(StsAssumeRoleCredentialsProvider::builder).thenReturn(stsCredentialsProviderBuilder);
                actualCredentialsProvider = objectUnderTest.providerFromOptions(awsCredentialsOptions);
            }

            assertThat(actualCredentialsProvider, instanceOf(StsAssumeRoleCredentialsProvider.class));

            final ArgumentCaptor<AssumeRoleRequest> assumeRoleRequestArgumentCaptor = ArgumentCaptor.forClass(AssumeRoleRequest.class);
            verify(stsCredentialsProviderBuilder).refreshRequest(assumeRoleRequestArgumentCaptor.capture());

            final AssumeRoleRequest actualAssumeRoleRequest = assumeRoleRequestArgumentCaptor.getValue();
            assertThat(actualAssumeRoleRequest.roleArn(), equalTo(testStsRole));
            assertThat(actualAssumeRoleRequest.roleSessionName(), startsWith("Data-Prepper-"));
            assertThat(actualAssumeRoleRequest.roleSessionName().length(), lessThanOrEqualTo(MAXIMUM_ROLE_SESSION_LENGTH));
            assertThat(actualAssumeRoleRequest.overrideConfiguration(), notNullValue());
            assertThat(actualAssumeRoleRequest.overrideConfiguration().isPresent(), equalTo(false));
        }

        @Test
        void providerFromOptions_should_not_set_externalId_when_externalId_is_null() {
            when(awsCredentialsOptions.getRegion()).thenReturn(Region.US_EAST_1);
            when(awsCredentialsOptions.getStsRoleArn()).thenReturn(testStsRole);
            when(awsCredentialsOptions.getStsExternalId()).thenReturn(null);

            when(stsClientBuilder.region(Region.US_EAST_1)).thenReturn(stsClientBuilder);

            final CredentialsProviderFactory objectUnderTest = createObjectUnderTest();
            final AwsCredentialsProvider actualCredentialsProvider;

            try (final MockedStatic<StsClient> stsClientMockedStatic = mockStatic(StsClient.class);
                 final MockedStatic<StsAssumeRoleCredentialsProvider> credentialsProviderMockedStatic = mockStatic(StsAssumeRoleCredentialsProvider.class)) {
                stsClientMockedStatic.when(StsClient::builder).thenReturn(stsClientBuilder);
                credentialsProviderMockedStatic.when(StsAssumeRoleCredentialsProvider::builder).thenReturn(stsCredentialsProviderBuilder);
                actualCredentialsProvider = objectUnderTest.providerFromOptions(awsCredentialsOptions);
            }

            assertThat(actualCredentialsProvider, instanceOf(StsAssumeRoleCredentialsProvider.class));

            final ArgumentCaptor<AssumeRoleRequest> assumeRoleRequestArgumentCaptor = ArgumentCaptor.forClass(AssumeRoleRequest.class);
            verify(stsCredentialsProviderBuilder).refreshRequest(assumeRoleRequestArgumentCaptor.capture());

            final AssumeRoleRequest actualAssumeRoleRequest = assumeRoleRequestArgumentCaptor.getValue();
            assertThat(actualAssumeRoleRequest.roleArn(), equalTo(testStsRole));
            assertThat(actualAssumeRoleRequest.roleSessionName(), startsWith("Data-Prepper-"));
            assertThat(actualAssumeRoleRequest.roleSessionName().length(), lessThanOrEqualTo(MAXIMUM_ROLE_SESSION_LENGTH));
            assertThat(actualAssumeRoleRequest.externalId(), nullValue());
        }

        @Test
        void providerFromOptions_should_not_set_externalId_when_externalId_is_empty() {
            when(awsCredentialsOptions.getRegion()).thenReturn(Region.US_EAST_1);
            when(awsCredentialsOptions.getStsRoleArn()).thenReturn(testStsRole);
            when(awsCredentialsOptions.getStsExternalId()).thenReturn("");

            when(stsClientBuilder.region(Region.US_EAST_1)).thenReturn(stsClientBuilder);

            final CredentialsProviderFactory objectUnderTest = createObjectUnderTest();
            final AwsCredentialsProvider actualCredentialsProvider;

            try (final MockedStatic<StsClient> stsClientMockedStatic = mockStatic(StsClient.class);
                 final MockedStatic<StsAssumeRoleCredentialsProvider> credentialsProviderMockedStatic = mockStatic(StsAssumeRoleCredentialsProvider.class)) {
                stsClientMockedStatic.when(StsClient::builder).thenReturn(stsClientBuilder);
                credentialsProviderMockedStatic.when(StsAssumeRoleCredentialsProvider::builder).thenReturn(stsCredentialsProviderBuilder);
                actualCredentialsProvider = objectUnderTest.providerFromOptions(awsCredentialsOptions);
            }

            assertThat(actualCredentialsProvider, instanceOf(StsAssumeRoleCredentialsProvider.class));

            final ArgumentCaptor<AssumeRoleRequest> assumeRoleRequestArgumentCaptor = ArgumentCaptor.forClass(AssumeRoleRequest.class);
            verify(stsCredentialsProviderBuilder).refreshRequest(assumeRoleRequestArgumentCaptor.capture());

            final AssumeRoleRequest actualAssumeRoleRequest = assumeRoleRequestArgumentCaptor.getValue();
            assertThat(actualAssumeRoleRequest.roleArn(), equalTo(testStsRole));
            assertThat(actualAssumeRoleRequest.roleSessionName(), startsWith("Data-Prepper-"));
            assertThat(actualAssumeRoleRequest.roleSessionName().length(), lessThanOrEqualTo(MAXIMUM_ROLE_SESSION_LENGTH));
            assertThat(actualAssumeRoleRequest.externalId(), nullValue());
        }


        @ParameterizedTest
        @ValueSource(strings = {"us-east-1", "us-west-2", "eu-west-1"})
        void providerFromOptions_should_create_StsClient_with_correct_backoff(final String regionString) {
            final Region region = Region.of(regionString);
            when(awsCredentialsOptions.getRegion()).thenReturn(region);
            when(awsCredentialsOptions.getStsRoleArn()).thenReturn(testStsRole);

            when(stsClientBuilder.region(region)).thenReturn(stsClientBuilder);

            when(stsCredentialsProviderBuilder.stsClient(stsClient)).thenReturn(stsCredentialsProviderBuilder);
            when(stsCredentialsProviderBuilder.build()).thenReturn(stsCredentialsProvider);

            final CredentialsProviderFactory objectUnderTest = createObjectUnderTest();
            final AwsCredentialsProvider actualCredentialsProvider;
            try (final MockedStatic<StsClient> stsClientMockedStatic = mockStatic(StsClient.class);
                 final MockedStatic<StsAssumeRoleCredentialsProvider> credentialsProviderMockedStatic = mockStatic(StsAssumeRoleCredentialsProvider.class)) {
                stsClientMockedStatic.when(StsClient::builder).thenReturn(stsClientBuilder);
                credentialsProviderMockedStatic.when(StsAssumeRoleCredentialsProvider::builder).thenReturn(stsCredentialsProviderBuilder);
                actualCredentialsProvider = objectUnderTest.providerFromOptions(awsCredentialsOptions);
            }

            assertThat(actualCredentialsProvider, instanceOf(StsAssumeRoleCredentialsProvider.class));
            assertThat(actualCredentialsProvider, equalTo(stsCredentialsProvider));

            final ArgumentCaptor<ClientOverrideConfiguration> clientOverrideConfigurationArgumentCaptor =
                    ArgumentCaptor.forClass(ClientOverrideConfiguration.class);
            verify(stsClientBuilder).overrideConfiguration(clientOverrideConfigurationArgumentCaptor.capture());

            final ClientOverrideConfiguration actualOverrideConfiguration = clientOverrideConfigurationArgumentCaptor.getValue();
            assertThat(actualOverrideConfiguration.retryPolicy(), notNullValue());
            assertThat(actualOverrideConfiguration.retryPolicy().isPresent(), equalTo(true));
            final RetryPolicy retryPolicy = actualOverrideConfiguration.retryPolicy().get();
            assertThat(retryPolicy.numRetries(), equalTo(CredentialsProviderFactory.STS_CLIENT_RETRIES));
            assertThat(retryPolicy.retryCondition(), equalTo(RetryCondition.defaultRetryCondition()));

            final BackoffStrategy backoffStrategy = retryPolicy.backoffStrategy();
            assertThat(backoffStrategy, notNullValue());
            assertThat(backoffStrategy, instanceOf(EqualJitterBackoffStrategy.class));

            assertThat(retryPolicy.throttlingBackoffStrategy(), sameInstance(backoffStrategy));
        }
    }

    private String createStsRole() {
        return String.format("arn:aws:iam::123456789012:role/%s", UUID.randomUUID());
    }
}