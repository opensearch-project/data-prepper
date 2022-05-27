/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.plugins.source.configuration.AwsAuthenticationOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sts.StsClient;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SqsClientAuthenticationTest {

    S3SourceConfig s3SourceConfig;
    SqsClientAuthentication sqsClientAuthentication;
    AwsAuthenticationOptions awsAuthenticationOptions;
    StsClient stsClient = mock(StsClient.class);

    @BeforeEach
    void setUp() {
        s3SourceConfig = mock(S3SourceConfig.class);

        awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);

        when(s3SourceConfig.getAWSAuthentication()).thenReturn(awsAuthenticationOptions);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn("us-east-1");
        sqsClientAuthentication = new SqsClientAuthentication(s3SourceConfig);
    }

    @Test
    void createS3Client() {
        when(s3SourceConfig.getAWSAuthentication()).thenReturn(mock(AwsAuthenticationOptions.class));
        when(s3SourceConfig.getAWSAuthentication().getAwsRegion()).thenReturn("us-east-1");

        assertThat(s3SourceConfig.getAWSAuthentication().getAwsRegion(), equalTo("us-east-1"));
        assertThat(sqsClientAuthentication.createSqsClient(stsClient), instanceOf(SqsClient.class));
    }
}
