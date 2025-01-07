/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.sqs;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class SqsEventProcessorTest {

    private SqsMessageHandler mockSqsMessageHandler;
    private SqsEventProcessor sqsEventProcessor;
    private Buffer<Record<Event>> mockBuffer;
    private int mockBufferTimeoutMillis;
    private AcknowledgementSet mockAcknowledgementSet;

    @BeforeEach
    void setUp() {
        mockSqsMessageHandler = Mockito.mock(SqsMessageHandler.class);
        mockBuffer = Mockito.mock(Buffer.class);
        mockAcknowledgementSet = Mockito.mock(AcknowledgementSet.class);
        mockBufferTimeoutMillis = 10000;
        sqsEventProcessor = new SqsEventProcessor(mockSqsMessageHandler);
    }

    @Test
    void addSqsObject_callsHandleMessageWithCorrectParameters() throws IOException {
        Message message = Message.builder().body("Test Message Body").build();
        String queueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue";
        sqsEventProcessor.addSqsObject(message, queueUrl, mockBuffer, mockBufferTimeoutMillis, mockAcknowledgementSet);
        verify(mockSqsMessageHandler, times(1)).handleMessage(message, queueUrl, mockBuffer, mockBufferTimeoutMillis, mockAcknowledgementSet);
    }

    @Test
    void addSqsObject_propagatesIOExceptionThrownByHandleMessage() throws IOException {
        Message message = Message.builder().body("Test Message Body").build();
        String queueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue";
        doThrow(new IOException("Handle message failed")).when(mockSqsMessageHandler).handleMessage(message, queueUrl, mockBuffer, mockBufferTimeoutMillis, mockAcknowledgementSet);
        IOException thrownException = assertThrows(IOException.class, () ->
                sqsEventProcessor.addSqsObject(message, queueUrl, mockBuffer, mockBufferTimeoutMillis, mockAcknowledgementSet)
        );
        assert(thrownException.getMessage().equals("Handle message failed"));
        verify(mockSqsMessageHandler, times(1)).handleMessage(message, queueUrl, mockBuffer, mockBufferTimeoutMillis, mockAcknowledgementSet);
    }
}
