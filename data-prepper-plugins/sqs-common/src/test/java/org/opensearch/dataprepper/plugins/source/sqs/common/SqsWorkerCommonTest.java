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
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchResponse;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.time.Duration;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

class SqsWorkerCommonTest {
    private SqsClient sqsClient;
    private Backoff backoff;
    private PluginMetrics pluginMetrics;
    private AcknowledgementSetManager acknowledgementSetManager;
    private SqsWorkerCommon sqsWorkerCommon;

    @BeforeEach
    void setUp() {
        sqsClient = Mockito.mock(SqsClient.class);
        backoff = Mockito.mock(Backoff.class);
        pluginMetrics = Mockito.mock(PluginMetrics.class);
        acknowledgementSetManager = Mockito.mock(AcknowledgementSetManager.class);
        when(pluginMetrics.counter(Mockito.anyString())).thenReturn(Mockito.mock(Counter.class));
        when(pluginMetrics.timer(Mockito.anyString())).thenReturn(Mockito.mock(Timer.class));
        sqsWorkerCommon = new SqsWorkerCommon(sqsClient, backoff, pluginMetrics, acknowledgementSetManager);
    }

    @Test
    void testPollSqsMessages_handlesEmptyList() {
        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(ReceiveMessageResponse.builder()
                        .messages(Collections.emptyList())
                        .build());
        var messages = sqsWorkerCommon.pollSqsMessages(
                "testQueueUrl",
                10,
                Duration.ofSeconds(5),
                Duration.ofSeconds(30)
        );

        assertNotNull(messages);
        assertTrue(messages.isEmpty());
        Mockito.verify(sqsClient).receiveMessage(any(ReceiveMessageRequest.class));
        Mockito.verify(backoff, Mockito.never()).nextDelayMillis(Mockito.anyInt());
    }

    @Test
    void testDeleteSqsMessages_callsClientWhenNotStopped() {
        var entries = Collections.singletonList(
                DeleteMessageBatchRequestEntry.builder()
                        .id("msg-id")
                        .receiptHandle("receipt-handle")
                        .build()
        );

        when(sqsClient.deleteMessageBatch(any(DeleteMessageBatchRequest.class)))
                .thenReturn(DeleteMessageBatchResponse.builder().build());

        sqsWorkerCommon.deleteSqsMessages("testQueueUrl", entries);
        ArgumentCaptor<DeleteMessageBatchRequest> captor =
                ArgumentCaptor.forClass(DeleteMessageBatchRequest.class);
        Mockito.verify(sqsClient).deleteMessageBatch(captor.capture());
        assertEquals("testQueueUrl", captor.getValue().queueUrl());
        assertEquals(1, captor.getValue().entries().size());
    }

    @Test
    void testStop_skipsFurtherOperations() {
        sqsWorkerCommon.stop();
        sqsWorkerCommon.deleteSqsMessages("testQueueUrl", Collections.singletonList(
                DeleteMessageBatchRequestEntry.builder()
                        .id("msg-id")
                        .receiptHandle("receipt-handle")
                        .build()
        ));
        Mockito.verify(sqsClient, Mockito.never()).deleteMessageBatch((DeleteMessageBatchRequest) any());
    }
}
