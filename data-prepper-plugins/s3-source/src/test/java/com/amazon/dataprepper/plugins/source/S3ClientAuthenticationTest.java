/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.plugins.source.configuration.AwsAuthenticationOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sts.StsClient;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class S3ClientAuthenticationTest {

    S3SourceConfig s3SourceConfig;
    S3ClientAuthentication s3ClientAuthentication;
    AwsAuthenticationOptions awsAuthenticationOptions;
    StsClient stsClient = mock(StsClient.class);

    @BeforeEach
    void setUp() {
        s3SourceConfig = mock(S3SourceConfig.class);
        awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);

        when(s3SourceConfig.getAWSAuthentication()).thenReturn(awsAuthenticationOptions);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn("us-east-1");

        s3ClientAuthentication = new S3ClientAuthentication(s3SourceConfig);
    }

    @Test
    void createS3Client() {
        assertThat(s3SourceConfig.getAWSAuthentication().getAwsRegion(), equalTo("us-east-1"));
        assertThat(s3ClientAuthentication.createS3Client(stsClient), instanceOf(software.amazon.awssdk.services.s3.S3Client.class));
    }
}
