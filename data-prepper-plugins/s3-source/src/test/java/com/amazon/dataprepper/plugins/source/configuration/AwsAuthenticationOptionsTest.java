/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.configuration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;

import java.lang.reflect.Field;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

class AwsAuthenticationOptionsTest {

    private AwsAuthenticationOptions awsAuthenticationOptions;

    @BeforeEach
    void setUp() {
        awsAuthenticationOptions = new AwsAuthenticationOptions();
    }

    @ParameterizedTest
    @ValueSource(strings = {"us-east-1", "us-east-2", "ap-northeast-1", "fake-dynamic-2"})
    void getAwsRegion_returns_Region(final String region) throws NoSuchFieldException, IllegalAccessException {
        reflectivelySetField(awsAuthenticationOptions, "awsRegion", region);
        assertThat(awsAuthenticationOptions.getAwsRegion(), equalTo(Region.of(region)));
    }

    @Test
    void getAwsRegion_returns_null_when_region_is_null() throws NoSuchFieldException, IllegalAccessException {
        reflectivelySetField(awsAuthenticationOptions, "awsRegion", null);
        assertThat(awsAuthenticationOptions.getAwsRegion(), nullValue());
    }

    @Test
    void authenticateAWSConfiguration_should_return_s3Client_without_sts_role_arn() throws NoSuchFieldException, IllegalAccessException {
        reflectivelySetField(awsAuthenticationOptions, "awsRegion", "us-east-1");
        reflectivelySetField(awsAuthenticationOptions, "awsStsRoleArn", null);

        final DefaultCredentialsProvider mockedCredentialsProvider = mock(DefaultCredentialsProvider.class);
        final AwsCredentialsProvider actualCredentialsProvider;
        try (final MockedStatic<DefaultCredentialsProvider> defaultCredentialsProviderMockedStatic = mockStatic(DefaultCredentialsProvider.class)) {
            defaultCredentialsProviderMockedStatic.when(DefaultCredentialsProvider::create)
                    .thenReturn(mockedCredentialsProvider);
            actualCredentialsProvider = awsAuthenticationOptions.authenticateAwsConfiguration();
        }

        assertThat(actualCredentialsProvider, sameInstance(mockedCredentialsProvider));
    }

    @Nested
    class WithSts {
        private StsClient stsClient;
        private StsClientBuilder stsClientBuilder;

        @BeforeEach
        void setUp() {
            stsClient = mock(StsClient.class);
            stsClientBuilder = mock(StsClientBuilder.class);

            when(stsClientBuilder.build()).thenReturn(stsClient);
        }


        @Test
        void authenticateAWSConfiguration_should_return_s3Client_with_sts_role_arn() throws NoSuchFieldException, IllegalAccessException {
            reflectivelySetField(awsAuthenticationOptions, "awsRegion", "us-east-1");
            reflectivelySetField(awsAuthenticationOptions, "awsStsRoleArn", "arn:aws:iam::123456789012:iam-role");

            when(stsClientBuilder.region(Region.US_EAST_1)).thenReturn(stsClientBuilder);

            final AwsCredentialsProvider actualCredentialsProvider;
            try (final MockedStatic<StsClient> stsClientMockedStatic = mockStatic(StsClient.class)) {
                stsClientMockedStatic.when(StsClient::builder).thenReturn(stsClientBuilder);
                actualCredentialsProvider = awsAuthenticationOptions.authenticateAwsConfiguration();
            }

            assertThat(actualCredentialsProvider, instanceOf(AwsCredentialsProvider.class));
        }

        @Test
        void authenticateAWSConfiguration_should_return_s3Client_with_sts_role_arn_when_no_region() throws NoSuchFieldException, IllegalAccessException {
            reflectivelySetField(awsAuthenticationOptions, "awsRegion", null);
            reflectivelySetField(awsAuthenticationOptions, "awsStsRoleArn", "arn:aws:iam::123456789012:iam-role");
            assertThat(awsAuthenticationOptions.getAwsRegion(), equalTo(null));

            when(stsClientBuilder.region(null)).thenReturn(stsClientBuilder);

            final AwsCredentialsProvider actualCredentialsProvider;
            try (final MockedStatic<StsClient> stsClientMockedStatic = mockStatic(StsClient.class)) {
                stsClientMockedStatic.when(StsClient::builder).thenReturn(stsClientBuilder);
                actualCredentialsProvider = awsAuthenticationOptions.authenticateAwsConfiguration();
            }

            assertThat(actualCredentialsProvider, instanceOf(AwsCredentialsProvider.class));
        }
    }

    private void reflectivelySetField(final AwsAuthenticationOptions awsAuthenticationOptions, final String fieldName, final Object value) throws NoSuchFieldException, IllegalAccessException {
        final Field field = AwsAuthenticationOptions.class.getDeclaredField(fieldName);
        try {
            field.setAccessible(true);
            field.set(awsAuthenticationOptions, value);
        } finally {
            field.setAccessible(false);
        }
    }
}