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

class SqsServiceTest {

    S3SourceConfig s3SourceConfig;
    SqsService sqsService;
    AwsAuthenticationOptions awsAuthenticationOptions;
    StsClient stsClient = mock(StsClient.class);
    S3Service s3ServiceMock = mock(S3Service.class);

    @BeforeEach
    void setUp() {
        s3SourceConfig = mock(S3SourceConfig.class);

        awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);

        when(s3SourceConfig.getAWSAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn("us-east-1");
        sqsService = new SqsService(s3SourceConfig, s3ServiceMock);
    }

    @Test
    void createSqsClient_should_return_instance_of_S3Client() {
        when(s3SourceConfig.getAWSAuthenticationOptions()).thenReturn(mock(AwsAuthenticationOptions.class));
        when(s3SourceConfig.getAWSAuthenticationOptions().getAwsRegion()).thenReturn("us-east-1");

        assertThat(s3SourceConfig.getAWSAuthenticationOptions().getAwsRegion(), equalTo("us-east-1"));
        assertThat(sqsService.createSqsClient(stsClient), instanceOf(SqsClient.class));
    }

}