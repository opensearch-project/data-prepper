/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.sqs;

import com.linecorp.armeria.client.retry.Backoff;
import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.acknowledgements.ProgressCheck;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.sqs.common.SqsWorkerCommon;
import org.opensearch.dataprepper.plugins.source.sqs.common.SqsRetriesExhaustedException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResultEntry;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SqsException;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.UUID;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SqsWorkerTest {

    @Mock
    private Buffer<Record<Event>> buffer;
    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;
    @Mock
    private SqsClient sqsClient;
    @Mock
    private SqsEventProcessor sqsEventProcessor;
    @Mock
    private SqsSourceConfig sqsSourceConfig;
    @Mock
    private QueueConfig queueConfig;
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private PluginFactory pluginFactory;
    @Mock
    private Backoff backoff;
    @Mock
    private Counter sqsMessagesReceivedCounter;
    @Mock
    private Counter sqsMessagesDeletedCounter;
    @Mock
    private Counter sqsMessagesFailedCounter;
    @Mock
    private Counter sqsMessagesDeleteFailedCounter;
    @Mock
    private Counter acknowledgementSetCallbackCounter;
    @Mock
    private Counter sqsVisibilityTimeoutChangedCount;
    @Mock
    private Counter sqsVisibilityTimeoutChangeFailedCount;
    private final int mockBufferTimeoutMillis = 10000;

    private SqsWorker createObjectUnderTest() {
        return new SqsWorker(
                buffer,
                acknowledgementSetManager,
                sqsClient,
                sqsSourceConfig,
                queueConfig,
                pluginMetrics,
                sqsEventProcessor,
                backoff);
    }

    @BeforeEach
    void setUp() {
        when(pluginMetrics.counter(SqsWorkerCommon.SQS_MESSAGES_RECEIVED_METRIC_NAME))
                .thenReturn(sqsMessagesReceivedCounter);
        when(pluginMetrics.counter(SqsWorkerCommon.SQS_MESSAGES_DELETED_METRIC_NAME))
                .thenReturn(sqsMessagesDeletedCounter);
        when(pluginMetrics.counter(SqsWorkerCommon.SQS_MESSAGES_FAILED_METRIC_NAME))
                .thenReturn(sqsMessagesFailedCounter);
        when(pluginMetrics.counter(SqsWorkerCommon.SQS_MESSAGES_DELETE_FAILED_METRIC_NAME))
                .thenReturn(sqsMessagesDeleteFailedCounter);
        when(pluginMetrics.counter(SqsWorkerCommon.ACKNOWLEDGEMENT_SET_CALLACK_METRIC_NAME))
                .thenReturn(acknowledgementSetCallbackCounter);
        when(pluginMetrics.counter(SqsWorkerCommon.SQS_VISIBILITY_TIMEOUT_CHANGED_COUNT_METRIC_NAME))
                .thenReturn(sqsVisibilityTimeoutChangedCount);
        when(pluginMetrics.counter(SqsWorkerCommon.SQS_VISIBILITY_TIMEOUT_CHANGE_FAILED_COUNT_METRIC_NAME))
                .thenReturn(sqsVisibilityTimeoutChangeFailedCount);
        when(sqsSourceConfig.getAcknowledgements()).thenReturn(false);
        when(sqsSourceConfig.getBufferTimeout()).thenReturn(Duration.ofSeconds(10));
        when(queueConfig.getUrl()).thenReturn("https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue");
        when(queueConfig.getWaitTime()).thenReturn(Duration.ofSeconds(1));
    }

    @Test
    void processSqsMessages_should_return_number_of_messages_processed_and_increment_counters() throws IOException {
        final Message message = Message.builder()
                .messageId(UUID.randomUUID().toString())
                .receiptHandle(UUID.randomUUID().toString())
                .body("{\"Records\":[{\"eventSource\":\"custom\",\"message\":\"Hello World\"}]}")
                .build();

        final ReceiveMessageResponse response = ReceiveMessageResponse.builder().messages(message).build();
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(response);

        final DeleteMessageBatchResultEntry successfulDelete = DeleteMessageBatchResultEntry.builder().id(message.messageId()).build();
        final DeleteMessageBatchResponse deleteResponse = DeleteMessageBatchResponse.builder().successful(successfulDelete).build();
        when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class))).thenReturn(deleteResponse);

        int messagesProcessed = createObjectUnderTest().processSqsMessages();
        assertThat(messagesProcessed, equalTo(1));

        verify(sqsMessagesReceivedCounter).increment(1);
        verify(sqsMessagesDeletedCounter).increment(1);
        verify(sqsMessagesDeleteFailedCounter, never()).increment(anyDouble());
    }

    @Test
    void processSqsMessages_should_invoke_processSqsEvent_and_deleteSqsMessages_when_entries_non_empty() throws IOException {
        final Message message = Message.builder()
                .messageId(UUID.randomUUID().toString())
                .receiptHandle(UUID.randomUUID().toString())
                .body("{\"Records\":[{\"eventSource\":\"custom\",\"message\":\"Hello World\"}]}")
                .build();

        final ReceiveMessageResponse response = ReceiveMessageResponse.builder()
                .messages(message)
                .build();
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(response);

        final DeleteMessageBatchResultEntry successfulDelete = DeleteMessageBatchResultEntry.builder()
                .id(message.messageId())
                .build();
        final DeleteMessageBatchResponse deleteResponse = DeleteMessageBatchResponse.builder()
                .successful(successfulDelete)
                .build();
        when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class))).thenReturn(deleteResponse);

        SqsWorker sqsWorker = createObjectUnderTest();
        int messagesProcessed = sqsWorker.processSqsMessages();

        assertThat(messagesProcessed, equalTo(1));
        verify(sqsEventProcessor, times(1)).addSqsObject(eq(message), eq("https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue"), eq(buffer), eq(mockBufferTimeoutMillis), isNull());
        verify(sqsClient, times(1)).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
        verify(sqsMessagesReceivedCounter).increment(1);
        verify(sqsMessagesDeletedCounter).increment(1);
        verify(sqsMessagesDeleteFailedCounter, never()).increment(anyDouble());
    }

    @Test
    void processSqsMessages_should_not_invoke_processSqsEvent_and_deleteSqsMessages_when_entries_are_empty() throws IOException {
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder().messages(Collections.emptyList()).build());
        SqsWorker sqsWorker = createObjectUnderTest();
        int messagesProcessed = sqsWorker.processSqsMessages();
        assertThat(messagesProcessed, equalTo(0));
        verify(sqsEventProcessor, never()).addSqsObject(any(), anyString(), any(), anyInt(), any());
        verify(sqsClient, never()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
        verify(sqsMessagesReceivedCounter, never()).increment(anyDouble());
        verify(sqsMessagesDeletedCounter, never()).increment(anyDouble());
    }


    @Test
    void processSqsMessages_should_not_delete_messages_if_acknowledgements_enabled_until_acknowledged() throws IOException {
        when(sqsSourceConfig.getAcknowledgements()).thenReturn(true);
        AcknowledgementSet acknowledgementSet = mock(AcknowledgementSet.class);
        when(acknowledgementSetManager.create(any(), any())).thenReturn(acknowledgementSet);
        when(queueConfig.getUrl()).thenReturn("https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue");

        final Message message = Message.builder()
                .messageId("msg-1")
                .receiptHandle("rh-1")
                .body("{\"Records\":[{\"eventSource\":\"custom\",\"message\":\"Hello World\"}]}")
                .build();

        final ReceiveMessageResponse response = ReceiveMessageResponse.builder().messages(message).build();
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(response);
        int messagesProcessed = createObjectUnderTest().processSqsMessages();
        assertThat(messagesProcessed, equalTo(1));
        verify(sqsEventProcessor, times(1)).addSqsObject(eq(message),
                eq("https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue"),
                eq(buffer),
                eq(mockBufferTimeoutMillis),
                eq(acknowledgementSet));
        verify(sqsMessagesReceivedCounter).increment(1);
        verifyNoInteractions(sqsMessagesDeletedCounter);
    }

    @Test
    void acknowledgementsEnabled_and_visibilityDuplicateProtectionEnabled_should_create_ack_sets_and_progress_check() {
        when(sqsSourceConfig.getAcknowledgements()).thenReturn(true);
        when(queueConfig.getVisibilityDuplicateProtection()).thenReturn(true);

        SqsWorker worker = new SqsWorker(buffer, acknowledgementSetManager, sqsClient, sqsSourceConfig, queueConfig, pluginMetrics, sqsEventProcessor, backoff);
        Message message = Message.builder().messageId("msg-dup").receiptHandle("handle-dup").build();
        ReceiveMessageResponse response = ReceiveMessageResponse.builder().messages(message).build();
        when(sqsClient.receiveMessage((ReceiveMessageRequest) any())).thenReturn(response);

        AcknowledgementSet acknowledgementSet = mock(AcknowledgementSet.class);
        when(acknowledgementSetManager.create(any(), any())).thenReturn(acknowledgementSet);

        int processed = worker.processSqsMessages();
        assertThat(processed, equalTo(1));

        verify(acknowledgementSetManager).create(any(), any());
        verify(acknowledgementSet).addProgressCheck(any(), any());
    }

    @Test
    void processSqsMessages_should_return_zero_messages_with_backoff_when_a_SqsException_is_thrown() {
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenThrow(SqsException.class);
        final int messagesProcessed = createObjectUnderTest().processSqsMessages();
        verify(backoff).nextDelayMillis(1);
        assertThat(messagesProcessed, equalTo(0));
    }

    @Test
    void processSqsMessages_should_throw_when_a_SqsException_is_thrown_with_max_retries() {
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenThrow(SqsException.class);
        when(backoff.nextDelayMillis(anyInt())).thenReturn((long) -1);
        SqsWorker objectUnderTest = createObjectUnderTest();
        assertThrows(SqsRetriesExhaustedException.class, objectUnderTest::processSqsMessages);
    }

    @Test
    void processSqsMessages_should_update_visibility_timeout_when_progress_changes() throws IOException {
        AcknowledgementSet acknowledgementSet = mock(AcknowledgementSet.class);
        when(queueConfig.getVisibilityDuplicateProtection()).thenReturn(true);
        when(queueConfig.getVisibilityTimeout()).thenReturn(Duration.ofMillis(1));
        when(acknowledgementSetManager.create(any(), any(Duration.class))).thenReturn(acknowledgementSet);
        when(sqsSourceConfig.getAcknowledgements()).thenReturn(true);
        final Message message = mock(Message.class);
        final String testReceiptHandle = UUID.randomUUID().toString();
        when(message.messageId()).thenReturn(testReceiptHandle);
        when(message.receiptHandle()).thenReturn(testReceiptHandle);

        final ReceiveMessageResponse receiveMessageResponse = mock(ReceiveMessageResponse.class);
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(receiveMessageResponse);
        when(receiveMessageResponse.messages()).thenReturn(Collections.singletonList(message));

        final int messagesProcessed = createObjectUnderTest().processSqsMessages();

        assertThat(messagesProcessed, equalTo(1));
        verify(sqsEventProcessor).addSqsObject(any(), anyString(), any(), anyInt(), any());
        verify(acknowledgementSetManager).create(any(), any(Duration.class));

        ArgumentCaptor<Consumer<ProgressCheck>> progressConsumerArgumentCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(acknowledgementSet).addProgressCheck(progressConsumerArgumentCaptor.capture(), any(Duration.class));
        final Consumer<ProgressCheck> actualConsumer = progressConsumerArgumentCaptor.getValue();
        final ProgressCheck progressCheck = mock(ProgressCheck.class);
        actualConsumer.accept(progressCheck);

        ArgumentCaptor<ChangeMessageVisibilityRequest> changeMessageVisibilityRequestArgumentCaptor = ArgumentCaptor.forClass(ChangeMessageVisibilityRequest.class);
        verify(sqsClient).changeMessageVisibility(changeMessageVisibilityRequestArgumentCaptor.capture());
        ChangeMessageVisibilityRequest actualChangeVisibilityRequest = changeMessageVisibilityRequestArgumentCaptor.getValue();
        assertThat(actualChangeVisibilityRequest.queueUrl(), equalTo(queueConfig.getUrl()));
        assertThat(actualChangeVisibilityRequest.receiptHandle(), equalTo(testReceiptHandle));
        verify(sqsMessagesReceivedCounter).increment(1);
    }
    @Test
    void increaseVisibilityTimeout_doesNothing_whenIsStopped() throws IOException {
        when(sqsSourceConfig.getAcknowledgements()).thenReturn(true);
        when(queueConfig.getVisibilityDuplicateProtection()).thenReturn(false);
        when(queueConfig.getVisibilityTimeout()).thenReturn(Duration.ofSeconds(30));
        AcknowledgementSet mockAcknowledgementSet = mock(AcknowledgementSet.class);
        when(acknowledgementSetManager.create(any(), any())).thenReturn(mockAcknowledgementSet);
        Message message = Message.builder()
                .messageId(UUID.randomUUID().toString())
                .receiptHandle(UUID.randomUUID().toString())
                .body("{\"Records\":[{\"eventSource\":\"custom\",\"message\":\"Hello World\"}]}")
                .build();
        ReceiveMessageResponse response = ReceiveMessageResponse.builder()
                .messages(message)
                .build();
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(response);
        SqsWorker sqsWorker = createObjectUnderTest();
        sqsWorker.stop();
        int messagesProcessed = sqsWorker.processSqsMessages();
        assertThat(messagesProcessed, equalTo(1));
        verify(sqsEventProcessor, times(1)).addSqsObject(eq(message),
                eq("https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue"),
                eq(buffer),
                eq(mockBufferTimeoutMillis),
                eq(mockAcknowledgementSet));
        verify(sqsClient, never()).changeMessageVisibility(any(ChangeMessageVisibilityRequest.class));
        verify(sqsVisibilityTimeoutChangeFailedCount, never()).increment();
    }

    @Test
    void deleteSqsMessages_incrementsFailedCounter_whenDeleteResponseHasFailedDeletes() throws IOException {
        final Message message1 = Message.builder()
                .messageId(UUID.randomUUID().toString())
                .receiptHandle(UUID.randomUUID().toString())
                .body("{\"Records\":[{\"eventSource\":\"custom\",\"message\":\"Hello World 1\"}]}")
                .build();
        final Message message2 = Message.builder()
                .messageId(UUID.randomUUID().toString())
                .receiptHandle(UUID.randomUUID().toString())
                .body("{\"Records\":[{\"eventSource\":\"custom\",\"message\":\"Hello World 2\"}]}")
                .build();

        final ReceiveMessageResponse receiveResponse = ReceiveMessageResponse.builder()
                .messages(message1, message2)
                .build();
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(receiveResponse);

        DeleteMessageBatchResultEntry successfulDelete = DeleteMessageBatchResultEntry.builder()
                .id(message1.messageId())
                .build();

        BatchResultErrorEntry failedDelete = BatchResultErrorEntry.builder()
                .id(message2.messageId())
                .code("ReceiptHandleIsInvalid")
                .senderFault(true)
                .message("Failed to delete message due to invalid receipt handle.")
                .build();

        DeleteMessageBatchResponse deleteResponse = DeleteMessageBatchResponse.builder()
                .successful(successfulDelete)
                .failed(failedDelete)
                .build();

        when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class))).thenReturn(deleteResponse);
        SqsWorker sqsWorker = createObjectUnderTest();
        int messagesProcessed = sqsWorker.processSqsMessages();
        assertThat(messagesProcessed, equalTo(2));
        verify(sqsMessagesReceivedCounter).increment(2);
        verify(sqsMessagesDeletedCounter).increment(1);
        verify(sqsMessagesDeleteFailedCounter).increment(1);
    }
    @Test
    void processSqsMessages_handlesException_correctly_when_addSqsObject_throwsException() throws IOException {
        final Message message = Message.builder()
                .messageId(UUID.randomUUID().toString())
                .receiptHandle(UUID.randomUUID().toString())
                .body("{\"Records\":[{\"eventSource\":\"custom\",\"message\":\"Hello World\"}]}")
                .build();
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(
                ReceiveMessageResponse.builder().messages(message).build()
        );
        doThrow(new RuntimeException("Processing failed")).when(sqsEventProcessor)
                .addSqsObject(eq(message), eq("https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue"),
                        any(), anyInt(), any());
        SqsWorker sqsWorker = createObjectUnderTest();
        int messagesProcessed = sqsWorker.processSqsMessages();
        assertThat(messagesProcessed, equalTo(1));
        verify(sqsMessagesReceivedCounter).increment(1);
        verify(sqsMessagesFailedCounter).increment();
        verify(backoff).nextDelayMillis(anyInt());
        verify(sqsClient, never()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
        verify(sqsMessagesDeletedCounter, never()).increment(anyInt());
    }
}