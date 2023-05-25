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
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CredentialsProviderFactoryTest {
    @Mock
    private AwsCredentialsOptions awsCredentialsOptions;

    @BeforeEach
    void setUp() {

    }

    private CredentialsProviderFactory createObjectUnderTest() {
        return new CredentialsProviderFactory();
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


    @Nested
    class WithSts {
        private StsClient stsClient;
        private StsClientBuilder stsClientBuilder;
        private String testStsRole;

        @BeforeEach
        void setUp() {
            stsClient = mock(StsClient.class);
            stsClientBuilder = mock(StsClientBuilder.class);

            when(stsClientBuilder.build()).thenReturn(stsClient);

            testStsRole = createStsRole();
        }

        @Test
        void authenticateAWSConfiguration_should_return_s3Client_with_sts_role_arn() {
            when(awsCredentialsOptions.getRegion()).thenReturn(Region.US_EAST_1);
            when(awsCredentialsOptions.getStsRoleArn()).thenReturn(testStsRole);

            when(stsClientBuilder.region(Region.US_EAST_1)).thenReturn(stsClientBuilder);
            final AssumeRoleRequest.Builder assumeRoleRequestBuilder = mock(AssumeRoleRequest.Builder.class);
            when(assumeRoleRequestBuilder.roleSessionName(anyString()))
                    .thenReturn(assumeRoleRequestBuilder);
            when(assumeRoleRequestBuilder.roleArn(anyString()))
                    .thenReturn(assumeRoleRequestBuilder);


            final CredentialsProviderFactory objectUnderTest = createObjectUnderTest();
            final AwsCredentialsProvider actualCredentialsProvider;
            try (final MockedStatic<StsClient> stsClientMockedStatic = mockStatic(StsClient.class);
                 final MockedStatic<AssumeRoleRequest> assumeRoleRequestMockedStatic = mockStatic(AssumeRoleRequest.class)) {
                stsClientMockedStatic.when(StsClient::builder).thenReturn(stsClientBuilder);
                assumeRoleRequestMockedStatic.when(AssumeRoleRequest::builder).thenReturn(assumeRoleRequestBuilder);
                actualCredentialsProvider = objectUnderTest.providerFromOptions(awsCredentialsOptions);
            }

            assertThat(actualCredentialsProvider, instanceOf(AwsCredentialsProvider.class));

            verify(assumeRoleRequestBuilder).roleArn(testStsRole);
            verify(assumeRoleRequestBuilder).roleSessionName(anyString());
            verify(assumeRoleRequestBuilder).build();
            verifyNoMoreInteractions(assumeRoleRequestBuilder);
        }

        @Test
        void authenticateAWSConfiguration_should_return_s3Client_with_sts_role_arn_when_no_region() {
            when(awsCredentialsOptions.getRegion()).thenReturn(null);
            when(awsCredentialsOptions.getStsRoleArn()).thenReturn(testStsRole);

            final CredentialsProviderFactory objectUnderTest = createObjectUnderTest();
            final AwsCredentialsProvider actualCredentialsProvider;
            try (final MockedStatic<StsClient> stsClientMockedStatic = mockStatic(StsClient.class)) {
                stsClientMockedStatic.when(StsClient::builder).thenReturn(stsClientBuilder);
                actualCredentialsProvider = objectUnderTest.providerFromOptions(awsCredentialsOptions);
            }

            assertThat(actualCredentialsProvider, instanceOf(AwsCredentialsProvider.class));
        }

        @Test
        void authenticateAWSConfiguration_should_override_STS_Headers_when_HeaderOverrides_when_set() {
            final String headerName1 = UUID.randomUUID().toString();
            final String headerValue1 = UUID.randomUUID().toString();
            final String headerName2 = UUID.randomUUID().toString();
            final String headerValue2 = UUID.randomUUID().toString();
            final Map<String, String> overrideHeaders = Map.of(headerName1, headerValue1, headerName2, headerValue2);

            when(awsCredentialsOptions.getRegion()).thenReturn(Region.US_EAST_1);
            when(awsCredentialsOptions.getStsRoleArn()).thenReturn(testStsRole);
            when(awsCredentialsOptions.getStsHeaderOverrides()).thenReturn(overrideHeaders);

            when(stsClientBuilder.region(Region.US_EAST_1)).thenReturn(stsClientBuilder);

            final AssumeRoleRequest.Builder assumeRoleRequestBuilder = mock(AssumeRoleRequest.Builder.class);
            when(assumeRoleRequestBuilder.roleSessionName(anyString()))
                    .thenReturn(assumeRoleRequestBuilder);
            when(assumeRoleRequestBuilder.roleArn(anyString()))
                    .thenReturn(assumeRoleRequestBuilder);
            when(assumeRoleRequestBuilder.overrideConfiguration(any(Consumer.class)))
                    .thenReturn(assumeRoleRequestBuilder);

            final CredentialsProviderFactory objectUnderTest = createObjectUnderTest();
            final AwsCredentialsProvider actualCredentialsProvider;
            try (final MockedStatic<StsClient> stsClientMockedStatic = mockStatic(StsClient.class);
                 final MockedStatic<AssumeRoleRequest> assumeRoleRequestMockedStatic = mockStatic(AssumeRoleRequest.class)) {
                stsClientMockedStatic.when(StsClient::builder).thenReturn(stsClientBuilder);
                assumeRoleRequestMockedStatic.when(AssumeRoleRequest::builder).thenReturn(assumeRoleRequestBuilder);
                actualCredentialsProvider = objectUnderTest.providerFromOptions(awsCredentialsOptions);
            }

            assertThat(actualCredentialsProvider, instanceOf(AwsCredentialsProvider.class));

            final ArgumentCaptor<Consumer<AwsRequestOverrideConfiguration.Builder>> configurationCaptor = ArgumentCaptor.forClass(Consumer.class);

            verify(assumeRoleRequestBuilder).roleArn(testStsRole);
            verify(assumeRoleRequestBuilder).roleSessionName(anyString());
            verify(assumeRoleRequestBuilder).overrideConfiguration(configurationCaptor.capture());
            verify(assumeRoleRequestBuilder).build();
            verifyNoMoreInteractions(assumeRoleRequestBuilder);

            final Consumer<AwsRequestOverrideConfiguration.Builder> actualOverride = configurationCaptor.getValue();

            final AwsRequestOverrideConfiguration.Builder configurationBuilder = mock(AwsRequestOverrideConfiguration.Builder.class);
            actualOverride.accept(configurationBuilder);
            verify(configurationBuilder).putHeader(headerName1, headerValue1);
            verify(configurationBuilder).putHeader(headerName2, headerValue2);
            verifyNoMoreInteractions(configurationBuilder);
        }

        @Test
        void authenticateAWSConfiguration_should_not_override_STS_Headers_when_HeaderOverrides_are_empty() {
            when(awsCredentialsOptions.getRegion()).thenReturn(Region.US_EAST_1);
            when(awsCredentialsOptions.getStsRoleArn()).thenReturn(testStsRole);
            when(awsCredentialsOptions.getStsHeaderOverrides()).thenReturn(Collections.emptyMap());

            when(stsClientBuilder.region(Region.US_EAST_1)).thenReturn(stsClientBuilder);
            final AssumeRoleRequest.Builder assumeRoleRequestBuilder = mock(AssumeRoleRequest.Builder.class);
            when(assumeRoleRequestBuilder.roleSessionName(anyString()))
                    .thenReturn(assumeRoleRequestBuilder);
            when(assumeRoleRequestBuilder.roleArn(anyString()))
                    .thenReturn(assumeRoleRequestBuilder);

            final CredentialsProviderFactory objectUnderTest = createObjectUnderTest();
            final AwsCredentialsProvider actualCredentialsProvider;
            try (final MockedStatic<StsClient> stsClientMockedStatic = mockStatic(StsClient.class);
                 final MockedStatic<AssumeRoleRequest> assumeRoleRequestMockedStatic = mockStatic(AssumeRoleRequest.class)) {
                stsClientMockedStatic.when(StsClient::builder).thenReturn(stsClientBuilder);
                assumeRoleRequestMockedStatic.when(AssumeRoleRequest::builder).thenReturn(assumeRoleRequestBuilder);
                actualCredentialsProvider = objectUnderTest.providerFromOptions(awsCredentialsOptions);
            }

            assertThat(actualCredentialsProvider, instanceOf(AwsCredentialsProvider.class));

            verify(assumeRoleRequestBuilder).roleArn(testStsRole);
            verify(assumeRoleRequestBuilder).roleSessionName(anyString());
            verify(assumeRoleRequestBuilder).build();
            verifyNoMoreInteractions(assumeRoleRequestBuilder);
        }
    }

    private String createStsRole() {
        return String.format("arn:aws:iam::123456789012:role/%s", UUID.randomUUID());
    }
}