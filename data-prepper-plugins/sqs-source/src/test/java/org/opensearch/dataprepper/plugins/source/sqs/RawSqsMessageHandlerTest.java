/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.sqs;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import software.amazon.awssdk.services.sqs.model.Message;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class RawSqsMessageHandlerTest {

    private final RawSqsMessageHandler rawSqsMessageHandler = new RawSqsMessageHandler();
    private Buffer<Record<Event>> mockBuffer;
    private int mockBufferTimeoutMillis;

    @BeforeEach
    void setUp() {
        mockBuffer = mock(Buffer.class);
        mockBufferTimeoutMillis = 10000;
    }

    @Test
    void handleMessage_callsBufferWriteOnce() throws Exception {
        Message message = Message.builder().body("{\"key\":\"value\"}").build();
        String queueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue";
        rawSqsMessageHandler.handleMessage(message, queueUrl, mockBuffer, mockBufferTimeoutMillis, null);
        ArgumentCaptor<Record<Event>> argumentCaptor = ArgumentCaptor.forClass(Record.class);
        verify(mockBuffer, times(1)).write(argumentCaptor.capture(), eq(mockBufferTimeoutMillis));
        Record<Event> capturedRecord = argumentCaptor.getValue();
        assertEquals("DOCUMENT", capturedRecord.getData().getMetadata().getEventType(), "Event type should be 'DOCUMENT'");
    }
}
