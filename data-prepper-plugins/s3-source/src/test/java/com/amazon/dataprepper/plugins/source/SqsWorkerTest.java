/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.plugins.source.configuration.AwsAuthenticationOptions;
import com.amazon.dataprepper.plugins.source.configuration.SqsOptions;
import com.amazon.dataprepper.plugins.source.filter.ObjectCreatedFilter;
import com.amazon.dataprepper.plugins.source.filter.S3EventFilter;
import com.amazonaws.services.s3.event.S3EventNotification;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SqsException;

import java.util.Collections;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SqsWorkerTest {
    private SqsWorker sqsWorker;
    private SqsClient sqsClient;
    private S3Service s3Service;
    private S3SourceConfig s3SourceConfig;
    private S3EventFilter objectCreatedFilter;

    @BeforeEach
    void setUp() {
        sqsClient = mock(SqsClient.class);
        s3Service = mock(S3Service.class);
        s3SourceConfig = mock(S3SourceConfig.class);
        objectCreatedFilter = new ObjectCreatedFilter();

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
    void getMessagesFromSqs_throws_null_pointer_exception_with_dummy_queue_url() {
        assertThrows(NullPointerException.class, () -> sqsWorker.getMessagesFromSqs());
    }

    @Test
    void createReceiveMessageRequest_should_return_ReceiveMessageRequest() {
        assertThat(sqsWorker.createReceiveMessageRequest(), instanceOf(ReceiveMessageRequest.class));
    }

    @Test
    void processSqsMessages_should_return_number_of_messages_processed() {
        final Message message = mock(Message.class);
        when(message.body()).thenReturn("{\"Records\":[{\"eventVersion\":\"2.1\",\"eventSource\":\"aws:s3\",\"awsRegion\":\"us-east-1\"," +
                "\"eventTime\":\"2022-06-06T18:02:33.495Z\",\"eventName\":\"ObjectCreated:Put\",\"userIdentity\":{\"principalId\":\"AWS:AROAX:xxxxxx\"}," +
                "\"requestParameters\":{\"sourceIPAddress\":\"99.99.999.99\"},\"responseElements\":{\"x-amz-request-id\":\"ABCD\"," +
                "\"x-amz-id-2\":\"abcd\"},\"s3\":{\"s3SchemaVersion\":\"1.0\",\"configurationId\":\"s3SourceEventNotification\"," +
                "\"bucket\":{\"name\":\"bucketName\",\"ownerIdentity\":{\"principalId\":\"ID\"},\"arn\":\"arn:aws:s3:::bucketName\"}," +
                "\"object\":{\"key\":\"File.gz\",\"size\":72,\"eTag\":\"abcd\",\"sequencer\":\"ABCD\"}}}]}");

        final ReceiveMessageResponse receiveMessageResponse = mock(ReceiveMessageResponse.class);
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(receiveMessageResponse);
        when(receiveMessageResponse.messages()).thenReturn(Collections.singletonList(message));

        final int messagesProcessed = sqsWorker.processSqsMessages();

        assertThat(messagesProcessed, equalTo(1));
    }

    @Test
    void processSqsMessages_should_return_zero_messages_when_a_SqsException_is_thrown() {
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenThrow(SqsException.class);
        final int messagesProcessed = sqsWorker.processSqsMessages();
        assertThat(messagesProcessed, equalTo(0));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "{\"foo\": \"bar\""})
    void processSqsMessages_should_throw_SdkClientException_if_input_is_not_valid_JSON(String inputString) {
        final Message message = mock(Message.class);
        when(message.body()).thenReturn("{\"foo\": \"bar\"");

        final ReceiveMessageResponse receiveMessageResponse = mock(ReceiveMessageResponse.class);
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(receiveMessageResponse);
        when(receiveMessageResponse.messages()).thenReturn(Collections.singletonList(message));

        final int messagesProcessed = sqsWorker.processSqsMessages();
        assertThat(messagesProcessed, equalTo(1));
    }

    @ParameterizedTest
    @ValueSource(strings = {"{\"foo\": \"bar\"}", "{}"})
    void processSqsMessages_should_return_zero_messages_when_messages_are_not_S3EventsNotificationRecords(String inputString) {
        final Message message = mock(Message.class);
        when(message.body()).thenReturn(inputString);

        final ReceiveMessageResponse receiveMessageResponse = mock(ReceiveMessageResponse.class);
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(receiveMessageResponse);
        when(receiveMessageResponse.messages()).thenReturn(Collections.singletonList(message));

        final int messagesProcessed = sqsWorker.processSqsMessages();
        assertThat(messagesProcessed, equalTo(1));
    }

    @Test
    void convertS3EventMessages_convert_message_to_S3EventNotificationRecord() {
        Message message = mock(Message.class);
        when(message.body()).thenReturn("{\"Records\":[{\"eventVersion\":\"2.1\",\"eventSource\":\"aws:s3\",\"awsRegion\":\"us-east-1\"," +
                "\"eventTime\":\"2022-06-06T18:02:33.495Z\",\"eventName\":\"ObjectCreated:Put\",\"userIdentity\":{\"principalId\":\"AWS:AROAX:xxxxxx\"}," +
                "\"requestParameters\":{\"sourceIPAddress\":\"99.99.999.99\"},\"responseElements\":{\"x-amz-request-id\":\"ABCD\"," +
                "\"x-amz-id-2\":\"abcd\"},\"s3\":{\"s3SchemaVersion\":\"1.0\",\"configurationId\":\"s3SourceEventNotification\"," +
                "\"bucket\":{\"name\":\"bucketName\",\"ownerIdentity\":{\"principalId\":\"ID\"},\"arn\":\"arn:aws:s3:::bucketName\"}," +
                "\"object\":{\"key\":\"File.gz\",\"size\":72,\"eTag\":\"abcd\",\"sequencer\":\"ABCD\"}}}]}");
        S3EventNotification.S3EventNotificationRecord actualS3EventNotificationRecord = sqsWorker.convertS3EventMessages(message);
        assertThat(actualS3EventNotificationRecord, instanceOf(S3EventNotification.S3EventNotificationRecord.class));
        assertThat(actualS3EventNotificationRecord.getAwsRegion(), equalTo("us-east-1"));
        assertThat(actualS3EventNotificationRecord.getS3().getBucket().getName(), equalTo("bucketName"));
        assertThat(actualS3EventNotificationRecord.getS3().getObject().getKey(), equalTo("File.gz"));
    }

    @Test
    void isEventNameCreated_should_return_true_if_event_is_created() {
        S3EventNotification.S3EventNotificationRecord s3EventNotificationRecord = mock(S3EventNotification.S3EventNotificationRecord.class);
        when(s3EventNotificationRecord.getEventName()).thenReturn("ObjectCreated");
        Assertions.assertTrue(sqsWorker.isEventNameCreated(s3EventNotificationRecord));
    }

    @Test
    void isEventNameCreated_should_return_false_if_event_is_not_created() {
        S3EventNotification.S3EventNotificationRecord s3EventNotificationRecord = mock(S3EventNotification.S3EventNotificationRecord.class);
        when(s3EventNotificationRecord.getEventName()).thenReturn("ObjectRemoved");
        Assertions.assertFalse(sqsWorker.isEventNameCreated(s3EventNotificationRecord));
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