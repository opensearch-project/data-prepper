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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
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
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.sqs.common.OnErrorOption;
import org.opensearch.dataprepper.plugins.source.sqs.common.SqsWorkerCommon;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SqsException;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
    private SqsWorkerCommon sqsWorkerCommon;
    @Mock
    private SqsEventProcessor sqsEventProcessor;
    @Mock
    private SqsSourceConfig sqsSourceConfig;
    @Mock
    private QueueConfig queueConfig;
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private Timer sqsMessageDelayTimer;
    @Mock
    private Counter sqsMessagesFailedCounter;
    private final int bufferTimeoutMillis = 10000;
    private SqsWorker sqsWorker;

    private SqsWorker createObjectUnderTest() {
        return new SqsWorker(
                buffer,
                acknowledgementSetManager,
                sqsClient,
                sqsWorkerCommon,
                sqsSourceConfig,
                queueConfig,
                pluginMetrics,
                sqsEventProcessor
        );
    }

    @BeforeEach
    void setUp() {
        when(sqsSourceConfig.getAcknowledgements()).thenReturn(false);
        when(sqsSourceConfig.getBufferTimeout()).thenReturn(Duration.ofSeconds(10));
        lenient().when(queueConfig.getUrl()).thenReturn("https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue");
        lenient().when(queueConfig.getWaitTime()).thenReturn(Duration.ofSeconds(10));
        lenient().when(queueConfig.getMaximumMessages()).thenReturn(10);
        lenient().when(queueConfig.getVisibilityTimeout()).thenReturn(Duration.ofSeconds(30));
        when(pluginMetrics.timer(SqsWorker.SQS_MESSAGE_DELAY_METRIC_NAME)).thenReturn(sqsMessageDelayTimer);
        lenient().doNothing().when(sqsMessageDelayTimer).record(any(Duration.class));
        sqsWorker = new SqsWorker(
                buffer,
                acknowledgementSetManager,
                sqsClient,
                sqsWorkerCommon,
                sqsSourceConfig,
                queueConfig,
                pluginMetrics,
                sqsEventProcessor
        );
        final Message message = Message.builder()
                .messageId("msg-1")
                .receiptHandle("rh-1")
                .body("{\"Records\":[{\"eventSource\":\"custom\",\"message\":\"Hello World\"}]}")
                .attributes(Map.of(
                        MessageSystemAttributeName.SENT_TIMESTAMP, "1234567890",
                        MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT, "0"
                ))
                .build();


        lenient().when(sqsWorkerCommon.pollSqsMessages(
                anyString(),
                eq(sqsClient),
                any(),
                any(),
                any()
        )).thenReturn(Collections.singletonList(message));

        lenient().when(sqsWorkerCommon.buildDeleteMessageBatchRequestEntry(anyString(), anyString()))
                .thenAnswer(invocation -> {
                    String messageId = invocation.getArgument(0);
                    String receiptHandle = invocation.getArgument(1);
                    return DeleteMessageBatchRequestEntry.builder()
                            .id(messageId)
                            .receiptHandle(receiptHandle)
                            .build();
                });
    }

    @Test
    void processSqsMessages_should_call_addSqsObject_and_deleteSqsMessages_for_valid_message() throws IOException {
        int messagesProcessed = sqsWorker.processSqsMessages();
        assertThat(messagesProcessed, equalTo(1));
        verify(sqsEventProcessor, times(1)).addSqsObject(
                any(),
                eq("https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue"),
                eq(buffer),
                anyInt(),
                isNull());
        verify(sqsWorkerCommon, atLeastOnce()).deleteSqsMessages(
                eq("https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue"),
                eq(sqsClient),
                anyList()
        );
        verify(sqsMessageDelayTimer, times(1)).record(any(Duration.class));
    }


    @Test
    void processSqsMessages_should_not_invoke_processSqsEvent_and_deleteSqsMessages_when_entries_are_empty() throws IOException {
        when(sqsWorkerCommon.pollSqsMessages(
                anyString(),
                eq(sqsClient),
                any(),
                any(),
                any()
        )).thenReturn(ReceiveMessageResponse.builder().messages(Collections.emptyList()).build().messages());

        int messagesProcessed = sqsWorker.processSqsMessages();

        assertThat(messagesProcessed, equalTo(0));
        verify(sqsEventProcessor, times(0)).addSqsObject(
                any(),
                eq("https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue"),
                eq(buffer),
                anyInt(),
                isNull());
        verify(sqsWorkerCommon, times(0)).deleteSqsMessages(
                eq("https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue"),
                eq(sqsClient),
                anyList()
        );
        verify(sqsMessageDelayTimer, times(1)).record(any(Duration.class));
    }


    @Test
    void processSqsMessages_should_not_delete_messages_if_acknowledgements_enabled_until_acknowledged() throws IOException {
        when(sqsSourceConfig.getAcknowledgements()).thenReturn(true);
        AcknowledgementSet acknowledgementSet = mock(AcknowledgementSet.class);
        when(acknowledgementSetManager.create(any(), any())).thenReturn(acknowledgementSet);
        when(queueConfig.getUrl()).thenReturn("https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue");
        int messagesProcessed = createObjectUnderTest().processSqsMessages();
        assertThat(messagesProcessed, equalTo(1));
        verify(sqsEventProcessor, times(1)).addSqsObject(any(),
                eq("https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue"),
                eq(buffer),
                eq(bufferTimeoutMillis),
                eq(acknowledgementSet));
        verify(sqsWorkerCommon, times(0)).deleteSqsMessages(
                eq("https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue"),
                eq(sqsClient),
                anyList()
        );
        verify(sqsMessageDelayTimer, times(1)).record(any(Duration.class));
    }

    @Test
    void acknowledgementsEnabled_and_visibilityDuplicateProtectionEnabled_should_create_ack_sets_and_progress_check() {
        when(sqsSourceConfig.getAcknowledgements()).thenReturn(true);
        when(queueConfig.getVisibilityDuplicateProtection()).thenReturn(true);
        AcknowledgementSet acknowledgementSet = mock(AcknowledgementSet.class);
        when(acknowledgementSetManager.create(any(), any())).thenReturn(acknowledgementSet);
        createObjectUnderTest().processSqsMessages();
        verify(acknowledgementSetManager).create(any(), any());
        verify(acknowledgementSet).addProgressCheck(any(), any());
    }

    @Test
    void processSqsMessages_should_return_zero_messages_with_backoff_when_a_SqsException_is_thrown() {
        when(sqsWorkerCommon.pollSqsMessages(
                anyString(),
                eq(sqsClient),
                any(),
                any(),
                any()
        )).thenThrow(SqsException.class);
        final int messagesProcessed = createObjectUnderTest().processSqsMessages();
        verify(sqsWorkerCommon, times(1)).applyBackoff();
        assertThat(messagesProcessed, equalTo(0));
    }


    @Test
    void processSqsMessages_should_update_visibility_timeout_when_progress_changes() throws IOException {
        when(sqsSourceConfig.getAcknowledgements()).thenReturn(true);
        when(queueConfig.getVisibilityDuplicateProtection()).thenReturn(true);
        when(queueConfig.getVisibilityDuplicateProtectionTimeout()).thenReturn(Duration.ofSeconds(60));
        when(queueConfig.getVisibilityTimeout()).thenReturn(Duration.ofSeconds(30));
        when(queueConfig.getUrl()).thenReturn("https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue");
        AcknowledgementSet acknowledgementSet = mock(AcknowledgementSet.class);
        when(acknowledgementSetManager.create(any(), any(Duration.class)))
                .thenReturn(acknowledgementSet);
        final String testMessageId = "msg-1";
        final String testReceiptHandle = "rh-1";

        SqsWorker sqsWorker = createObjectUnderTest(); // your builder method
        final int messagesProcessed = sqsWorker.processSqsMessages();
        assertThat(messagesProcessed, equalTo(1));

        verify(sqsEventProcessor).addSqsObject(
                any(),
                eq("https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue"),
                eq(buffer),
                eq(bufferTimeoutMillis),
                eq(acknowledgementSet)
        );
        verify(acknowledgementSetManager).create(any(), any(Duration.class));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Consumer<ProgressCheck>> progressConsumerCaptor = ArgumentCaptor.forClass(Consumer.class);
        verify(acknowledgementSet).addProgressCheck(progressConsumerCaptor.capture(), any(Duration.class));
        final Consumer<ProgressCheck> actualConsumer = progressConsumerCaptor.getValue();
        ProgressCheck progressCheck = mock(ProgressCheck.class);
        actualConsumer.accept(progressCheck);
        verify(sqsWorkerCommon).increaseVisibilityTimeout(
                eq("https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue"),
                eq(sqsClient),
                eq(testReceiptHandle),
                eq(30),
                eq(testMessageId)
        );
    }
    @Test
    void processSqsMessages_should_return_delete_message_entry_when_exception_thrown_and_onErrorOption_is_DELETE_MESSAGES() throws IOException {
        when(queueConfig.getOnErrorOption()).thenReturn(OnErrorOption.DELETE_MESSAGES);
        doThrow(new RuntimeException("Processing error"))
                .when(sqsEventProcessor).addSqsObject(any(),
                        anyString(),
                        eq(buffer),
                        anyInt(),
                        isNull());

        when(sqsWorkerCommon.getSqsMessagesFailedCounter()).thenReturn(sqsMessagesFailedCounter);
        SqsWorker worker = createObjectUnderTest();
        int messagesProcessed = worker.processSqsMessages();
        assertThat(messagesProcessed, equalTo(1));
        verify(sqsMessagesFailedCounter, times(1)).increment();
        verify(sqsWorkerCommon, atLeastOnce()).applyBackoff();
        verify(sqsWorkerCommon, atLeastOnce()).deleteSqsMessages(
                eq("https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue"),
                eq(sqsClient),
                anyList());
    }

    @Test
    void processSqsMessages_should_not_delete_message_entry_when_exception_thrown_and_onErrorOption_is_RETAIN_MESSAGES() throws IOException {
        when(queueConfig.getOnErrorOption()).thenReturn(OnErrorOption.RETAIN_MESSAGES);
        doThrow(new RuntimeException("Processing error"))
                .when(sqsEventProcessor).addSqsObject(any(),
                        anyString(),
                        eq(buffer),
                        anyInt(),
                        isNull());

        when(sqsWorkerCommon.getSqsMessagesFailedCounter()).thenReturn(sqsMessagesFailedCounter);
        SqsWorker worker = createObjectUnderTest();
        int messagesProcessed = worker.processSqsMessages();
        assertThat(messagesProcessed, equalTo(1));
        verify(sqsMessagesFailedCounter, times(1)).increment();
        verify(sqsWorkerCommon, atLeastOnce()).applyBackoff();
        verify(sqsWorkerCommon, times(0)).deleteSqsMessages(
                eq("https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue"),
                eq(sqsClient),
                anyList());
    }

    @Test
    void stop_should_set_isStopped_and_call_stop_on_sqsWorkerCommon() {
        SqsWorker worker = createObjectUnderTest();
        worker.stop();
        verify(sqsWorkerCommon, times(1)).stop();
    }

    @Test
    void processSqsMessages_should_record_sqsMessageDelayTimer_when_approximateReceiveCount_less_than_or_equal_to_one() throws IOException {
        final long sentTimestampMillis = Instant.now().minusSeconds(5).toEpochMilli();
        final Message message = Message.builder()
                .messageId("msg-1")
                .receiptHandle("rh-1")
                .body("{\"Records\":[{\"eventSource\":\"custom\",\"message\":\"Hello World\"}]}")
                .attributes(Map.of(
                        MessageSystemAttributeName.SENT_TIMESTAMP, String.valueOf(sentTimestampMillis),
                        MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT, "1"
                ))
                .build();
        when(sqsWorkerCommon.pollSqsMessages(anyString(), eq(sqsClient), anyInt(), any(), any()))
                .thenReturn(Collections.singletonList(message));

        SqsWorker worker = createObjectUnderTest();
        worker.processSqsMessages();
        ArgumentCaptor<Duration> durationCaptor = ArgumentCaptor.forClass(Duration.class);
        verify(sqsMessageDelayTimer).record(durationCaptor.capture());
    }

    @Test
    void processSqsMessages_should_not_record_sqsMessageDelayTimer_when_approximateReceiveCount_greater_than_one() throws IOException {
        final long sentTimestampMillis = Instant.now().minusSeconds(5).toEpochMilli();
        final Message message = Message.builder()
                .messageId("msg-1")
                .receiptHandle("rh-1")
                .body("{\"Records\":[{\"eventSource\":\"custom\",\"message\":\"Hello World\"}]}")
                .attributes(Map.of(
                        MessageSystemAttributeName.SENT_TIMESTAMP, String.valueOf(sentTimestampMillis),
                        MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT, "2"
                ))
                .build();
        when(sqsWorkerCommon.pollSqsMessages(anyString(), eq(sqsClient), anyInt(), any(), any()))
                .thenReturn(Collections.singletonList(message));

        SqsWorker worker = createObjectUnderTest();
        worker.processSqsMessages();
        verify(sqsMessageDelayTimer, never()).record(any(Duration.class));
    }
}