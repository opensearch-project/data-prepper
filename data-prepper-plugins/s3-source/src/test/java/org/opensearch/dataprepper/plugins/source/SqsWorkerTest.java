/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import com.linecorp.armeria.client.retry.Backoff;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.source.configuration.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.source.configuration.OnErrorOption;
import org.opensearch.dataprepper.plugins.source.configuration.SqsOptions;
import org.opensearch.dataprepper.plugins.source.exception.SqsRetriesExhaustedException;
import org.opensearch.dataprepper.plugins.source.filter.ObjectCreatedFilter;
import org.opensearch.dataprepper.plugins.source.filter.S3EventFilter;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResultEntry;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SqsException;
import software.amazon.awssdk.services.sts.model.StsException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.SqsWorker.SQS_MESSAGES_DELETED_METRIC_NAME;
import static org.opensearch.dataprepper.plugins.source.SqsWorker.SQS_MESSAGES_DELETE_FAILED_METRIC_NAME;
import static org.opensearch.dataprepper.plugins.source.SqsWorker.SQS_MESSAGES_FAILED_METRIC_NAME;
import static org.opensearch.dataprepper.plugins.source.SqsWorker.SQS_MESSAGES_RECEIVED_METRIC_NAME;
import static org.opensearch.dataprepper.plugins.source.SqsWorker.SQS_MESSAGE_DELAY_METRIC_NAME;

class SqsWorkerTest {
    private SqsWorker sqsWorker;
    private SqsClient sqsClient;
    private S3Service s3Service;
    private S3SourceConfig s3SourceConfig;
    private S3EventFilter objectCreatedFilter;
    private PluginMetrics pluginMetrics;
    private Backoff backoff;
    private Counter sqsMessagesReceivedCounter;
    private Counter sqsMessagesDeletedCounter;
    private Counter sqsMessagesFailedCounter;
    private Counter sqsMessagesDeleteFailedCounter;
    private Timer sqsMessageDelayTimer;
    private AcknowledgementSetManager acknowledgementSetManager;
    private AcknowledgementSet acknowledgementSet;
    private S3EventMessageParser s3EventMessageParser;

    @BeforeEach
    void setUp() {
        sqsClient = mock(SqsClient.class);
        s3Service = mock(S3Service.class);
        acknowledgementSetManager = mock(AcknowledgementSetManager.class);
        acknowledgementSet = mock(AcknowledgementSet.class);
        s3SourceConfig = mock(S3SourceConfig.class);
        objectCreatedFilter = new ObjectCreatedFilter();
        backoff = mock(Backoff.class);

        AwsAuthenticationOptions awsAuthenticationOptions = mock(AwsAuthenticationOptions.class);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.US_EAST_1);

        s3EventMessageParser = new S3EventMessageParser();

        SqsOptions sqsOptions = mock(SqsOptions.class);
        when(sqsOptions.getSqsUrl()).thenReturn("https://sqs.us-east-2.amazonaws.com/123456789012/MyQueue");

        when(s3SourceConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        when(s3SourceConfig.getSqsOptions()).thenReturn(sqsOptions);
        when(s3SourceConfig.getOnErrorOption()).thenReturn(OnErrorOption.RETAIN_MESSAGES);
        when(s3SourceConfig.getAcknowledgements()).thenReturn(false);

        pluginMetrics = mock(PluginMetrics.class);
        sqsMessagesReceivedCounter = mock(Counter.class);
        sqsMessagesDeletedCounter = mock(Counter.class);
        sqsMessagesFailedCounter = mock(Counter.class);
        sqsMessagesDeleteFailedCounter = mock(Counter.class);
        sqsMessageDelayTimer = mock(Timer.class);
        when(pluginMetrics.counter(SQS_MESSAGES_RECEIVED_METRIC_NAME)).thenReturn(sqsMessagesReceivedCounter);
        when(pluginMetrics.counter(SQS_MESSAGES_DELETED_METRIC_NAME)).thenReturn(sqsMessagesDeletedCounter);
        when(pluginMetrics.counter(SQS_MESSAGES_FAILED_METRIC_NAME)).thenReturn(sqsMessagesFailedCounter);
        when(pluginMetrics.counter(SQS_MESSAGES_DELETE_FAILED_METRIC_NAME)).thenReturn(sqsMessagesDeleteFailedCounter);
        when(pluginMetrics.timer(SQS_MESSAGE_DELAY_METRIC_NAME)).thenReturn(sqsMessageDelayTimer);

        sqsWorker = new SqsWorker(acknowledgementSetManager, sqsClient, s3Service, s3SourceConfig, pluginMetrics, s3EventMessageParser, backoff);
    }

    @AfterEach
    void cleanup() {
        verifyNoMoreInteractions(sqsMessagesReceivedCounter);
        verifyNoMoreInteractions(sqsMessagesDeletedCounter);
        verifyNoMoreInteractions(sqsMessagesFailedCounter);
        verifyNoMoreInteractions(sqsMessageDelayTimer);
    }

    @Nested
    class WithSuccessfulDeletion {
        @BeforeEach
        void setUp() {
            final DeleteMessageBatchResponse deleteMessageBatchResponse = mock(DeleteMessageBatchResponse.class);
            when(deleteMessageBatchResponse.hasSuccessful()).thenReturn(true);
            when(deleteMessageBatchResponse.successful()).thenReturn(Collections.singletonList(mock(DeleteMessageBatchResultEntry.class)));
            when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class))).thenReturn(deleteMessageBatchResponse);
        }

        @ParameterizedTest
        @ValueSource(strings = {"ObjectCreated:Put", "ObjectCreated:Post", "ObjectCreated:Copy", "ObjectCreated:CompleteMultipartUpload"})
        void processSqsMessages_should_return_number_of_messages_processed(final String eventName) throws IOException {
            Instant startTime = Instant.now().minus(1, ChronoUnit.HOURS);
            final Message message = mock(Message.class);
            when(message.body()).thenReturn(createEventNotification(eventName, startTime));
            final String testReceiptHandle = UUID.randomUUID().toString();
            when(message.messageId()).thenReturn(testReceiptHandle);
            when(message.receiptHandle()).thenReturn(testReceiptHandle);

            final ReceiveMessageResponse receiveMessageResponse = mock(ReceiveMessageResponse.class);
            when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(receiveMessageResponse);
            when(receiveMessageResponse.messages()).thenReturn(Collections.singletonList(message));

            final int messagesProcessed = sqsWorker.processSqsMessages();
            final ArgumentCaptor<DeleteMessageBatchRequest> deleteMessageBatchRequestArgumentCaptor = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
            verify(sqsClient).deleteMessageBatch(deleteMessageBatchRequestArgumentCaptor.capture());
            final DeleteMessageBatchRequest actualDeleteMessageBatchRequest = deleteMessageBatchRequestArgumentCaptor.getValue();

            final ArgumentCaptor<Duration> durationArgumentCaptor = ArgumentCaptor.forClass(Duration.class);
            verify(sqsMessageDelayTimer).record(durationArgumentCaptor.capture());
            Duration actualDelay = durationArgumentCaptor.getValue();

            assertThat(actualDeleteMessageBatchRequest, notNullValue());
            assertThat(actualDeleteMessageBatchRequest.entries().size(), equalTo(1));
            assertThat(actualDeleteMessageBatchRequest.queueUrl(), equalTo(s3SourceConfig.getSqsOptions().getSqsUrl()));
            assertThat(actualDeleteMessageBatchRequest.entries().get(0).id(), equalTo(message.messageId()));
            assertThat(actualDeleteMessageBatchRequest.entries().get(0).receiptHandle(), equalTo(message.receiptHandle()));
            assertThat(messagesProcessed, equalTo(1));
            verify(s3Service).addS3Object(any(S3ObjectReference.class), any());
            verify(sqsClient).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
            verify(sqsMessagesReceivedCounter).increment(1);
            verify(sqsMessagesDeletedCounter).increment(1);
            assertThat(actualDelay, lessThanOrEqualTo(Duration.ofHours(1).plus(Duration.ofSeconds(5))));
            assertThat(actualDelay, greaterThanOrEqualTo(Duration.ofHours(1).minus(Duration.ofSeconds(5))));
        }


        @ParameterizedTest
        @ValueSource(strings = {"ObjectCreated:Put", "ObjectCreated:Post", "ObjectCreated:Copy", "ObjectCreated:CompleteMultipartUpload"})
        void processSqsMessages_should_return_number_of_messages_processed_with_acknowledgements(final String eventName) throws IOException {
            when(acknowledgementSetManager.create(any(), any(Duration.class))).thenReturn(acknowledgementSet);
            when(s3SourceConfig.getAcknowledgements()).thenReturn(true);
            sqsWorker = new SqsWorker(acknowledgementSetManager, sqsClient, s3Service, s3SourceConfig, pluginMetrics, s3EventMessageParser, backoff);
            Instant startTime = Instant.now().minus(1, ChronoUnit.HOURS);
            final Message message = mock(Message.class);
            when(message.body()).thenReturn(createEventNotification(eventName, startTime));
            final String testReceiptHandle = UUID.randomUUID().toString();
            when(message.messageId()).thenReturn(testReceiptHandle);
            when(message.receiptHandle()).thenReturn(testReceiptHandle);

            final ReceiveMessageResponse receiveMessageResponse = mock(ReceiveMessageResponse.class);
            when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(receiveMessageResponse);
            when(receiveMessageResponse.messages()).thenReturn(Collections.singletonList(message));

            final int messagesProcessed = sqsWorker.processSqsMessages();
            final ArgumentCaptor<DeleteMessageBatchRequest> deleteMessageBatchRequestArgumentCaptor = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);

            final ArgumentCaptor<Duration> durationArgumentCaptor = ArgumentCaptor.forClass(Duration.class);
            verify(sqsMessageDelayTimer).record(durationArgumentCaptor.capture());
            Duration actualDelay = durationArgumentCaptor.getValue();

            assertThat(messagesProcessed, equalTo(1));
            verify(s3Service).addS3Object(any(S3ObjectReference.class), any());
            verify(acknowledgementSetManager).create(any(), any(Duration.class));
            verify(sqsMessagesReceivedCounter).increment(1);
            verifyNoInteractions(sqsMessagesDeletedCounter);
            assertThat(actualDelay, lessThanOrEqualTo(Duration.ofHours(1).plus(Duration.ofSeconds(5))));
            assertThat(actualDelay, greaterThanOrEqualTo(Duration.ofHours(1).minus(Duration.ofSeconds(5))));
        }

        @ParameterizedTest
        @ValueSource(strings = {"", "{\"foo\": \"bar\""})
        void processSqsMessages_should_not_interact_with_S3Service_if_input_is_not_valid_JSON(String inputString) {
            final Message message = mock(Message.class);
            when(message.body()).thenReturn(inputString);

            final ReceiveMessageResponse receiveMessageResponse = mock(ReceiveMessageResponse.class);
            when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(receiveMessageResponse);
            when(receiveMessageResponse.messages()).thenReturn(Collections.singletonList(message));

            final int messagesProcessed = sqsWorker.processSqsMessages();
            assertThat(messagesProcessed, equalTo(1));
            verifyNoInteractions(s3Service);
            verify(sqsClient, never()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
            verify(sqsMessagesReceivedCounter).increment(1);
            verify(sqsMessagesFailedCounter).increment();
        }

        @Test
        void processSqsMessages_should_not_interact_with_S3Service_and_delete_message_if_TestEvent() {
            final String messageId = UUID.randomUUID().toString();
            final String receiptHandle = UUID.randomUUID().toString();
            final Message message = mock(Message.class);
            when(message.body()).thenReturn("{\"Service\":\"Amazon S3\",\"Event\":\"s3:TestEvent\",\"Time\":\"2022-10-15T16:36:25.510Z\"," +
                    "\"Bucket\":\"bucketname\",\"RequestId\":\"abcdefg\",\"HostId\":\"hijklm\"}");
            when(message.messageId()).thenReturn(messageId);
            when(message.receiptHandle()).thenReturn(receiptHandle);

            final ReceiveMessageResponse receiveMessageResponse = mock(ReceiveMessageResponse.class);
            when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(receiveMessageResponse);
            when(receiveMessageResponse.messages()).thenReturn(Collections.singletonList(message));

            final int messagesProcessed = sqsWorker.processSqsMessages();
            assertThat(messagesProcessed, equalTo(1));
            verifyNoInteractions(s3Service);

            final ArgumentCaptor<DeleteMessageBatchRequest> deleteMessageBatchRequestArgumentCaptor = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
            verify(sqsClient).deleteMessageBatch(deleteMessageBatchRequestArgumentCaptor.capture());
            final DeleteMessageBatchRequest actualDeleteMessageBatchRequest = deleteMessageBatchRequestArgumentCaptor.getValue();

            assertThat(actualDeleteMessageBatchRequest, notNullValue());
            assertThat(actualDeleteMessageBatchRequest.entries().size(), equalTo(1));
            assertThat(actualDeleteMessageBatchRequest.queueUrl(), equalTo(s3SourceConfig.getSqsOptions().getSqsUrl()));
            assertThat(actualDeleteMessageBatchRequest.entries().get(0).id(), equalTo(messageId));
            assertThat(actualDeleteMessageBatchRequest.entries().get(0).receiptHandle(), equalTo(receiptHandle));
            assertThat(messagesProcessed, equalTo(1));

            verify(sqsMessagesReceivedCounter).increment(1);
            verify(sqsMessagesDeletedCounter).increment(1);
        }


        @ParameterizedTest
        @ValueSource(strings = {"ObjectRemoved:Delete", "ObjectRemoved:DeleteMarkerCreated"})
        void processSqsMessages_with_irrelevant_eventName_should_return_number_of_messages_processed_without_s3service_interactions(String eventName) {
            final Message message = mock(Message.class);
            when(message.body()).thenReturn(createEventNotification(eventName, Instant.now()));

            final ReceiveMessageResponse receiveMessageResponse = mock(ReceiveMessageResponse.class);
            when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(receiveMessageResponse);
            when(receiveMessageResponse.messages()).thenReturn(Collections.singletonList(message));

            final int messagesProcessed = sqsWorker.processSqsMessages();

            assertThat(messagesProcessed, equalTo(1));
            verifyNoInteractions(s3Service);
            verify(sqsMessagesReceivedCounter).increment(1);
            verify(sqsMessagesDeletedCounter).increment(1);
        }


        @ParameterizedTest
        @ValueSource(strings = {"", "{\"foo\": \"bar\""})
        void processSqsMessages_should_invoke_delete_if_input_is_not_valid_JSON_and_delete_on_error(String inputString) {
            when(s3SourceConfig.getOnErrorOption()).thenReturn(OnErrorOption.DELETE_MESSAGES);
            final Message message = mock(Message.class);
            when(message.body()).thenReturn(inputString);

            final String testReceiptHandle = UUID.randomUUID().toString();
            when(message.messageId()).thenReturn(testReceiptHandle);
            when(message.receiptHandle()).thenReturn(testReceiptHandle);

            final ReceiveMessageResponse receiveMessageResponse = mock(ReceiveMessageResponse.class);
            when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(receiveMessageResponse);
            when(receiveMessageResponse.messages()).thenReturn(Collections.singletonList(message));

            final int messagesProcessed = sqsWorker.processSqsMessages();
            final ArgumentCaptor<DeleteMessageBatchRequest> deleteMessageBatchRequestArgumentCaptor = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
            verify(sqsClient).deleteMessageBatch(deleteMessageBatchRequestArgumentCaptor.capture());
            final DeleteMessageBatchRequest actualDeleteMessageBatchRequest = deleteMessageBatchRequestArgumentCaptor.getValue();

            assertThat(actualDeleteMessageBatchRequest, notNullValue());
            assertThat(actualDeleteMessageBatchRequest.entries().size(), equalTo(1));
            assertThat(actualDeleteMessageBatchRequest.queueUrl(), equalTo(s3SourceConfig.getSqsOptions().getSqsUrl()));
            assertThat(actualDeleteMessageBatchRequest.entries().get(0).id(), equalTo(message.messageId()));
            assertThat(actualDeleteMessageBatchRequest.entries().get(0).receiptHandle(), equalTo(message.receiptHandle()));
            assertThat(messagesProcessed, equalTo(1));
            verifyNoInteractions(s3Service);
            verify(sqsClient, times(1)).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
            verify(sqsMessagesReceivedCounter).increment(1);
            verify(sqsMessagesDeletedCounter).increment(1);
            verify(sqsMessagesFailedCounter).increment();
        }
    }

    @Test
    void processSqsMessages_should_report_correct_metrics_for_DeleteMessages_when_some_succeed_and_some_fail() throws IOException {
        Instant startTime = Instant.now().minus(1, ChronoUnit.HOURS);
        final List<Message> messages = IntStream.range(0, 6).mapToObj(i -> {
                    final Message message = mock(Message.class);
                    when(message.body()).thenReturn(createPutNotification(startTime));
                    final String testReceiptHandle = UUID.randomUUID().toString();
                    when(message.messageId()).thenReturn(testReceiptHandle);
                    when(message.receiptHandle()).thenReturn(testReceiptHandle);
                    return message;
                })
                .collect(Collectors.toList());

        final ReceiveMessageResponse receiveMessageResponse = mock(ReceiveMessageResponse.class);
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(receiveMessageResponse);
        when(receiveMessageResponse.messages()).thenReturn(messages);

        final List<DeleteMessageBatchResultEntry> successfulDeletes = IntStream.range(0, 3)
                .mapToObj(i -> mock(DeleteMessageBatchResultEntry.class))
                .collect(Collectors.toList());
        final List<BatchResultErrorEntry> failedDeletes = IntStream.range(0, 3)
                .mapToObj(i -> mock(BatchResultErrorEntry.class))
                .collect(Collectors.toList());
        final DeleteMessageBatchResponse deleteMessageBatchResponse = mock(DeleteMessageBatchResponse.class);
        when(deleteMessageBatchResponse.hasSuccessful()).thenReturn(true);
        when(deleteMessageBatchResponse.successful()).thenReturn(successfulDeletes);
        when(deleteMessageBatchResponse.hasFailed()).thenReturn(true);
        when(deleteMessageBatchResponse.failed()).thenReturn(failedDeletes);
        when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class))).thenReturn(deleteMessageBatchResponse);

        final int messagesProcessed = sqsWorker.processSqsMessages();

        final ArgumentCaptor<DeleteMessageBatchRequest> deleteMessageBatchRequestArgumentCaptor = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
        verify(sqsClient).deleteMessageBatch(deleteMessageBatchRequestArgumentCaptor.capture());
        final DeleteMessageBatchRequest actualDeleteMessageBatchRequest = deleteMessageBatchRequestArgumentCaptor.getValue();

        assertThat(messagesProcessed, equalTo(6));
        assertThat(actualDeleteMessageBatchRequest, notNullValue());
        assertThat(actualDeleteMessageBatchRequest.entries().size(), equalTo(6));
        assertThat(actualDeleteMessageBatchRequest.queueUrl(), equalTo(s3SourceConfig.getSqsOptions().getSqsUrl()));
        verify(s3Service, times(6)).addS3Object(any(S3ObjectReference.class), any());
        verify(sqsClient).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
        verify(sqsMessagesReceivedCounter).increment(6);
        verify(sqsMessagesDeletedCounter).increment(3);
        verify(sqsMessagesDeleteFailedCounter).increment(3);

        verify(sqsMessageDelayTimer, times(6)).record(any(Duration.class));
    }

    @ParameterizedTest
    @ArgumentsSource(DeleteExceptionToBackoffCalls.class)
    void processSqsMessages_should_report_correct_metrics_for_DeleteMessages_when_request_fails(
            final Class<Exception> exClass, final int timesToCallBackoff) throws IOException {
        Instant startTime = Instant.now().minus(1, ChronoUnit.HOURS);
        final List<Message> messages = IntStream.range(0, 6).mapToObj(i -> {
                    final Message message = mock(Message.class);
                    when(message.body()).thenReturn(createPutNotification(startTime));
                    final String testReceiptHandle = UUID.randomUUID().toString();
                    when(message.messageId()).thenReturn(testReceiptHandle);
                    when(message.receiptHandle()).thenReturn(testReceiptHandle);
                    return message;
                })
                .collect(Collectors.toList());

        final ReceiveMessageResponse receiveMessageResponse = mock(ReceiveMessageResponse.class);
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(receiveMessageResponse);
        when(receiveMessageResponse.messages()).thenReturn(messages);

        when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class))).thenThrow(exClass);

        final int messagesProcessed = sqsWorker.processSqsMessages();

        final ArgumentCaptor<DeleteMessageBatchRequest> deleteMessageBatchRequestArgumentCaptor = ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
        verify(sqsClient).deleteMessageBatch(deleteMessageBatchRequestArgumentCaptor.capture());
        final DeleteMessageBatchRequest actualDeleteMessageBatchRequest = deleteMessageBatchRequestArgumentCaptor.getValue();

        assertThat(messagesProcessed, equalTo(6));
        assertThat(actualDeleteMessageBatchRequest, notNullValue());
        assertThat(actualDeleteMessageBatchRequest.entries().size(), equalTo(6));
        assertThat(actualDeleteMessageBatchRequest.queueUrl(), equalTo(s3SourceConfig.getSqsOptions().getSqsUrl()));
        verify(s3Service, times(6)).addS3Object(any(S3ObjectReference.class), any());
        verify(sqsClient).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
        verify(sqsMessagesReceivedCounter).increment(6);
        verifyNoInteractions(sqsMessagesDeletedCounter);
        verify(sqsMessagesDeleteFailedCounter).increment(6);

        verify(sqsMessageDelayTimer, times(6)).record(any(Duration.class));
        verify(backoff, times(timesToCallBackoff)).nextDelayMillis(1);
    }

    @Test
    void processSqsMessages_should_return_zero_messages_when_a_SqsException_is_thrown() {
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenThrow(SqsException.class);
        final int messagesProcessed = sqsWorker.processSqsMessages();
        assertThat(messagesProcessed, equalTo(0));
        verify(sqsClient, never()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
    }

    @Test
    void processSqsMessages_should_return_zero_messages_with_backoff_when_a_SqsException_is_thrown() {
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenThrow(SqsException.class);
        final int messagesProcessed = sqsWorker.processSqsMessages();
        verify(backoff).nextDelayMillis(1);
        assertThat(messagesProcessed, equalTo(0));
    }

    @Test
    void processSqsMessages_should_throw_when_a_SqsException_is_thrown_with_max_retries() {
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenThrow(SqsException.class);
        when(backoff.nextDelayMillis(anyInt())).thenReturn((long) -1);
        assertThrows(SqsRetriesExhaustedException.class, () -> sqsWorker.processSqsMessages());
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
        verifyNoInteractions(s3Service);
        verify(sqsClient, never()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
        verify(sqsMessagesReceivedCounter).increment(1);
        verify(sqsMessagesFailedCounter).increment();
    }

    @Test
    void populateS3Reference_should_interact_with_getUrlDecodedKey() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        // Using reflection to unit test a private method as part of bug fix.
        final Method method = SqsWorker.class.getDeclaredMethod("populateS3Reference", S3EventNotification.S3EventNotificationRecord.class);
        method.setAccessible(true);

        final S3EventNotification.S3EventNotificationRecord s3EventNotificationRecord = mock(S3EventNotification.S3EventNotificationRecord.class);
        final S3EventNotification.S3Entity s3Entity = mock(S3EventNotification.S3Entity.class);
        final S3EventNotification.S3ObjectEntity s3ObjectEntity = mock(S3EventNotification.S3ObjectEntity.class);
        final S3EventNotification.S3BucketEntity s3BucketEntity = mock(S3EventNotification.S3BucketEntity.class);

        when(s3EventNotificationRecord.getS3()).thenReturn(s3Entity);
        when(s3Entity.getBucket()).thenReturn(s3BucketEntity);
        when(s3Entity.getObject()).thenReturn(s3ObjectEntity);
        when(s3BucketEntity.getName()).thenReturn("test-bucket-name");
        when(s3ObjectEntity.getUrlDecodedKey()).thenReturn("test-key");

        final S3ObjectReference s3ObjectReference = (S3ObjectReference) method.invoke(sqsWorker, s3EventNotificationRecord);

        assertThat(s3ObjectReference, notNullValue());
        assertThat(s3ObjectReference.getBucketName(), equalTo("test-bucket-name"));
        assertThat(s3ObjectReference.getKey(), equalTo("test-key"));
        verify(s3ObjectEntity).getUrlDecodedKey();
        verifyNoMoreInteractions(s3ObjectEntity);
    }

    private static String createPutNotification(final Instant startTime) {
        return createEventNotification("ObjectCreated:Put", startTime);
    }

    private static String createEventNotification(final String eventName, final Instant startTime) {
        return "{\"Records\":[{\"eventVersion\":\"2.1\",\"eventSource\":\"aws:s3\",\"awsRegion\":\"us-east-1\"," +
                "\"eventTime\":\"" + startTime + "\",\"eventName\":\"" + eventName + "\",\"userIdentity\":{\"principalId\":\"AWS:AROAX:xxxxxx\"}," +
                "\"requestParameters\":{\"sourceIPAddress\":\"99.99.999.99\"},\"responseElements\":{\"x-amz-request-id\":\"ABCD\"," +
                "\"x-amz-id-2\":\"abcd\"},\"s3\":{\"s3SchemaVersion\":\"1.0\",\"configurationId\":\"s3SourceEventNotification\"," +
                "\"bucket\":{\"name\":\"bucketName\",\"ownerIdentity\":{\"principalId\":\"ID\"},\"arn\":\"arn:aws:s3:::bucketName\"}," +
                "\"object\":{\"key\":\"File.gz\",\"size\":72,\"eTag\":\"abcd\",\"sequencer\":\"ABCD\"}}}]}";
    }

    static class DeleteExceptionToBackoffCalls implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            return Stream.of(
                    arguments(SqsException.class, 0),
                    arguments(StsException.class, 1)
            );
        }
    }
}
