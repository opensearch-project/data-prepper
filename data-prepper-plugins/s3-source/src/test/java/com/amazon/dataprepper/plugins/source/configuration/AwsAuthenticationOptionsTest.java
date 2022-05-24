/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.configuration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;

import java.lang.reflect.Field;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

class AwsAuthenticationOptionsTest {

    private AwsAuthenticationOptions awsAuthenticationOptions;
    private final StsClient stsClient = mock(StsClient.class);

    @BeforeEach
    void setUp() {
        awsAuthenticationOptions = new AwsAuthenticationOptions();
    }

    @Test
    void authenticateAWSConfiguration_should_return_s3Client_without_sts_role_arn() throws NoSuchFieldException, IllegalAccessException {
        reflectivelySetField(awsAuthenticationOptions, "awsRegion", "us-east-1");
        reflectivelySetField(awsAuthenticationOptions, "awsStsRoleArn", null);
        assertThat(awsAuthenticationOptions.getAwsRegion(), equalTo("us-east-1"));
        assertThat(awsAuthenticationOptions.getAwsStsRoleArn(), equalTo(null));
        assertThat(awsAuthenticationOptions.authenticateAwsConfiguration(stsClient), instanceOf(AwsCredentialsProvider.class));
    }

    @Test
    void authenticateAWSConfiguration_should_return_s3Client_with_sts_role_arn() throws NoSuchFieldException, IllegalAccessException {
        reflectivelySetField(awsAuthenticationOptions, "awsRegion", "us-east-1");
        reflectivelySetField(awsAuthenticationOptions, "awsStsRoleArn", "arn:aws:iam::123456789012:iam-role");
        assertThat(awsAuthenticationOptions.getAwsRegion(), equalTo("us-east-1"));
        assertThat(awsAuthenticationOptions.getAwsStsRoleArn(), equalTo("arn:aws:iam::123456789012:iam-role"));
        assertThat(awsAuthenticationOptions.authenticateAwsConfiguration(stsClient), instanceOf(AwsCredentialsProvider.class));
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