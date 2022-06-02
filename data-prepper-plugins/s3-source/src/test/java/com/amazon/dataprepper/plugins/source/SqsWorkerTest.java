/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.plugins.source.configuration.AwsAuthenticationOptions;
import com.amazon.dataprepper.plugins.source.configuration.SqsOptions;
import com.amazonaws.services.s3.event.S3EventNotification;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.rmi.MarshalException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SqsWorkerTest {
    SqsWorker sqsWorker;
    SqsClient sqsClient;
    S3Service s3Service;
    S3SourceConfig s3SourceConfig;

    @BeforeEach
    void setUp() {
        sqsClient = mock(SqsClient.class);
        s3Service = mock(S3Service.class);
        s3SourceConfig = mock(S3SourceConfig.class);

        AwsAuthenticationOptions awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn("us-east-1");
        when(awsAuthenticationOptions.getAwsStsRoleArn()).thenReturn("arn:aws:iam::123456789012:iam-role");

        SqsOptions sqsOptions = mock(SqsOptions.class);
        when(sqsOptions.getSqsUrl()).thenReturn("https://sqs.us-east-2.amazonaws.com/123456789012/MyQueue");

        when(s3SourceConfig.getAWSAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        when(s3SourceConfig.getSqsOptions()).thenReturn(sqsOptions);

        sqsWorker = new SqsWorker(sqsClient, s3Service, s3SourceConfig);
    }

    @Test
    void createReceiveMessageRequest_should_return_ReceiveMessageRequest() {
        assertThat(sqsWorker.createReceiveMessageRequest(), instanceOf(ReceiveMessageRequest.class));
    }

    @Test
    void populateS3Reference_should_return_instance_of_S3ObjectReference() {
        S3EventNotification.S3EventNotificationRecord s3EventNotificationRecord = mock(S3EventNotification.S3EventNotificationRecord.class);
        S3EventNotification.S3Entity s3Entity = mock(S3EventNotification.S3Entity.class);
        S3EventNotification.S3BucketEntity s3BucketEntity = mock(S3EventNotification.S3BucketEntity.class);
        S3EventNotification.S3ObjectEntity s3ObjectEntity = mock(S3EventNotification.S3ObjectEntity.class);

        when(s3EventNotificationRecord.getS3()).thenReturn(s3Entity);
        when(s3Entity.getBucket()).thenReturn(s3BucketEntity);
        when(s3Entity.getObject()).thenReturn(s3ObjectEntity);

        when(s3EventNotificationRecord.getS3().getBucket().getName()).thenReturn("s3-source-test-bucket");
        when(s3EventNotificationRecord.getS3().getObject().getKey()).thenReturn("s3-bucket-key");

        S3ObjectReference s3ObjectReference = sqsWorker.populateS3Reference(s3EventNotificationRecord);

        assertThat(s3ObjectReference, instanceOf(S3ObjectReference.class));
        assertThat(s3ObjectReference.getBucketName(), equalTo("s3-source-test-bucket"));
        assertThat(s3ObjectReference.getKey(), equalTo("s3-bucket-key"));
    }
}