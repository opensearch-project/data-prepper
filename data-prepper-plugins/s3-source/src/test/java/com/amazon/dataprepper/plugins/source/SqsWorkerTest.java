/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.plugins.source.configuration.AwsAuthenticationOptions;
import com.amazon.dataprepper.plugins.source.configuration.SqsOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SqsWorkerTest {
    SqsWorker sqsWorker;
    SqsClient sqsClient;
    S3Client s3Client;
    S3SourceConfig s3SourceConfig;

    @BeforeEach
    void setUp() {
        sqsClient = mock(SqsClient.class);
        s3Client = mock(S3Client.class);
        s3SourceConfig = mock(S3SourceConfig.class);

        AwsAuthenticationOptions awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn("us-east-1");
        when(awsAuthenticationOptions.getAwsStsRoleArn()).thenReturn("arn:aws:iam::123456789012:iam-role");

        SqsOptions sqsOptions = mock(SqsOptions.class);
        when(sqsOptions.getSqsUrl()).thenReturn("https://sqs.us-east-2.amazonaws.com/123456789012/MyQueue");

        when(s3SourceConfig.getAWSAuthentication()).thenReturn(awsAuthenticationOptions);
        when(s3SourceConfig.getSqsOptions()).thenReturn(sqsOptions);

        sqsWorker = new SqsWorker(sqsClient, s3Client, s3SourceConfig);
    }

    @Test
    void createReceiveMessageRequest_should_return_ReceiveMessageRequest() {
        assertThat(sqsWorker.createReceiveMessageRequest(), instanceOf(ReceiveMessageRequest.class));
    }

}