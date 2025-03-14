/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.sqs.common;

import com.linecorp.armeria.client.retry.Backoff;
import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SqsException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


class SqsWorkerCommonTest {

    private SqsWorkerCommon sqsWorkerCommon;
    private PluginMetrics pluginMetrics;
    private AcknowledgementSetManager acknowledgementSetManager;
    private Backoff backoff;
    private Counter sqsMessagesReceivedCounter;
    private Counter sqsMessagesDeletedCounter;
    private Counter sqsMessagesFailedCounter;
    private Counter sqsMessagesDeleteFailedCounter;
    private Counter acknowledgementSetCallbackCounter;
    private Counter sqsVisibilityTimeoutChangedCount;
    private Counter sqsVisibilityTimeoutChangeFailedCount;
    private SqsClient sqsClient;
    private final String queueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue";

    @BeforeEach
    void setup() {
        pluginMetrics = mock(PluginMetrics.class);
        acknowledgementSetManager = mock(AcknowledgementSetManager.class);
        backoff = mock(Backoff.class);
        sqsMessagesReceivedCounter = mock(Counter.class);
        sqsMessagesDeletedCounter = mock(Counter.class);
        sqsMessagesFailedCounter = mock(Counter.class);
        sqsMessagesDeleteFailedCounter = mock(Counter.class);
        acknowledgementSetCallbackCounter = mock(Counter.class);
        sqsVisibilityTimeoutChangedCount = mock(Counter.class);
        sqsVisibilityTimeoutChangeFailedCount = mock(Counter.class);
        sqsClient = mock(SqsClient.class);
        when(pluginMetrics.counter(SqsWorkerCommon.SQS_MESSAGES_RECEIVED_METRIC_NAME)).thenReturn(sqsMessagesReceivedCounter);
        when(pluginMetrics.counter(SqsWorkerCommon.SQS_MESSAGES_DELETED_METRIC_NAME)).thenReturn(sqsMessagesDeletedCounter);
        when(pluginMetrics.counter(SqsWorkerCommon.SQS_MESSAGES_FAILED_METRIC_NAME)).thenReturn(sqsMessagesFailedCounter);
        when(pluginMetrics.counter(SqsWorkerCommon.SQS_MESSAGES_DELETE_FAILED_METRIC_NAME)).thenReturn(sqsMessagesDeleteFailedCounter);
        when(pluginMetrics.counter(SqsWorkerCommon.ACKNOWLEDGEMENT_SET_CALLACK_METRIC_NAME)).thenReturn(acknowledgementSetCallbackCounter);
        when(pluginMetrics.counter(SqsWorkerCommon.SQS_VISIBILITY_TIMEOUT_CHANGED_COUNT_METRIC_NAME)).thenReturn(sqsVisibilityTimeoutChangedCount);
        when(pluginMetrics.counter(SqsWorkerCommon.SQS_VISIBILITY_TIMEOUT_CHANGE_FAILED_COUNT_METRIC_NAME)).thenReturn(sqsVisibilityTimeoutChangeFailedCount);
        sqsWorkerCommon = new SqsWorkerCommon(backoff, pluginMetrics, acknowledgementSetManager);
    }

    @Test
    void testPollSqsMessages_successful() {
        Message message = Message.builder().messageId("msg-1").body("Test").build();
        List<Message> messages = Collections.singletonList(message);
        ReceiveMessageResponse response = ReceiveMessageResponse.builder().messages(messages).build();
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(response);
        List<Message> result = sqsWorkerCommon.pollSqsMessages(queueUrl, sqsClient, 10,
                Duration.ofSeconds(5), Duration.ofSeconds(30));

        verify(sqsMessagesReceivedCounter).increment(messages.size());
        assertThat(result, equalTo(messages));
    }

    @Test
    void testPollSqsMessages_exception() {
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenThrow(SqsException.builder().message("Error").build());
        when(backoff.nextDelayMillis(anyInt())).thenReturn(100L);
        List<Message> result = sqsWorkerCommon.pollSqsMessages(queueUrl, sqsClient, 10,
                Duration.ofSeconds(5), Duration.ofSeconds(30));
        assertThat(result, is(empty()));
        verify(sqsMessagesReceivedCounter, never()).increment(anyDouble());
    }

    @Test
    void testApplyBackoff_negativeDelayThrowsException() {
        when(backoff.nextDelayMillis(anyInt())).thenReturn(-1L);
        assertThrows(SqsRetriesExhaustedException.class, () -> sqsWorkerCommon.applyBackoff());
    }

    @Test
    void testApplyBackoff_positiveDelay() {
        when(backoff.nextDelayMillis(anyInt())).thenReturn(50L);
        assertDoesNotThrow(() -> sqsWorkerCommon.applyBackoff());
        verify(backoff).nextDelayMillis(anyInt());
    }

    @Test
    void testDeleteSqsMessages_noEntries() {
        sqsWorkerCommon.deleteSqsMessages(queueUrl, sqsClient, null);
        verify(sqsClient, never()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));

        sqsWorkerCommon.deleteSqsMessages(queueUrl, sqsClient, Collections.emptyList());
        verify(sqsClient, never()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));

        sqsWorkerCommon.stop();
        DeleteMessageBatchRequestEntry entry = DeleteMessageBatchRequestEntry.builder()
                .id("id").receiptHandle("rh").build();
        sqsWorkerCommon.deleteSqsMessages(queueUrl, sqsClient, Collections.singletonList(entry));
        verify(sqsClient, never()).deleteMessageBatch(any(DeleteMessageBatchRequest.class));
    }

    @Test
    void testDeleteSqsMessages_successfulDeletion_withConsumerBuilder() {
        DeleteMessageBatchRequestEntry entry = DeleteMessageBatchRequestEntry.builder()
                .id("id")
                .receiptHandle("rh")
                .build();
        List<DeleteMessageBatchRequestEntry> entries = Collections.singletonList(entry);
        DeleteMessageBatchResponse response = DeleteMessageBatchResponse.builder()
                .successful(builder -> builder.id("id"))
                .failed(Collections.emptyList())
                .build();
        when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class))).thenReturn(response);
        sqsWorkerCommon.deleteSqsMessages(queueUrl, sqsClient, entries);
        verify(sqsMessagesDeletedCounter).increment(1.0);
    }


    @Test
    void testDeleteSqsMessages_failedDeletion() {
        DeleteMessageBatchRequestEntry entry = DeleteMessageBatchRequestEntry.builder()
                .id("id").receiptHandle("rh").build();
        List<DeleteMessageBatchRequestEntry> entries = Collections.singletonList(entry);

        DeleteMessageBatchResponse response = DeleteMessageBatchResponse.builder()
                .successful(Collections.emptyList())
                .failed(Collections.singletonList(
                        BatchResultErrorEntry.builder().id("id").message("Failure").build()))
                .build();

        when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class))).thenReturn(response);
        sqsWorkerCommon.deleteSqsMessages(queueUrl, sqsClient, entries);
        verify(sqsMessagesDeleteFailedCounter).increment(1.0);
    }

    @Test
    void testDeleteSqsMessages_sdkException() {
        DeleteMessageBatchRequestEntry entry1 = DeleteMessageBatchRequestEntry.builder()
                .id("id-1").receiptHandle("rh-1").build();
        DeleteMessageBatchRequestEntry entry2 = DeleteMessageBatchRequestEntry.builder()
                .id("id-2").receiptHandle("rh-2").build();
        List<DeleteMessageBatchRequestEntry> entries = Arrays.asList(entry1, entry2);

        when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
                .thenThrow(SdkException.class);

        sqsWorkerCommon.deleteSqsMessages(queueUrl, sqsClient, entries);
        verify(sqsMessagesDeleteFailedCounter).increment(entries.size());
    }

    @Test
    void testIncreaseVisibilityTimeout_successful() {
        String receiptHandle = "rh";
        int newVisibilityTimeout = 45;
        String messageId = "msg";
        when(sqsClient.changeMessageVisibility(any(ChangeMessageVisibilityRequest.class)))
                .thenReturn(ChangeMessageVisibilityResponse.builder().build());

        sqsWorkerCommon.increaseVisibilityTimeout(queueUrl, sqsClient, receiptHandle,
                newVisibilityTimeout, messageId);

        ArgumentCaptor<ChangeMessageVisibilityRequest> requestCaptor =
                ArgumentCaptor.forClass(ChangeMessageVisibilityRequest.class);
        verify(sqsClient).changeMessageVisibility(requestCaptor.capture());
        ChangeMessageVisibilityRequest request = requestCaptor.getValue();
        assertThat(request.queueUrl(), equalTo(queueUrl));
        assertThat(request.receiptHandle(), equalTo(receiptHandle));
        assertThat(request.visibilityTimeout(), equalTo(newVisibilityTimeout));
        verify(sqsVisibilityTimeoutChangedCount).increment();
    }

    @Test
    void testIncreaseVisibilityTimeout_whenException() {
        String receiptHandle = "rh";
        int newVisibilityTimeout = 45;
        String messageId = "msg";
        doThrow(new RuntimeException("failure"))
                .when(sqsClient).changeMessageVisibility(any(ChangeMessageVisibilityRequest.class));

        sqsWorkerCommon.increaseVisibilityTimeout(queueUrl, sqsClient, receiptHandle,
                newVisibilityTimeout, messageId);

        verify(sqsVisibilityTimeoutChangeFailedCount).increment();
    }
}
