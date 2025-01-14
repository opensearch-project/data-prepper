/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.sqs;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.record.Record;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.InputStream;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BulkSqsMessageHandlerTest {

    private InputCodec mockCodec;
    private Buffer<Record<Event>> mockBuffer;
    private BulkSqsMessageHandler bulkSqsMessageHandler;
    private int bufferTimeoutMillis;

    @BeforeEach
    void setUp() {
        mockCodec = mock(InputCodec.class);
        mockBuffer = mock(Buffer.class);
        bulkSqsMessageHandler = new BulkSqsMessageHandler(mockCodec);
        bufferTimeoutMillis = 10000;
    }

    @Test
    void handleMessage_callsBufferWriteOnce() throws Exception {
        final Message message = Message.builder()
                .body("{\"someKey\":\"someValue\"}")
                .build();
        final String queueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue";

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            Consumer<Record<Event>> eventConsumer = invocation.getArgument(1);
            final Event mockEvent = mock(Event.class);
            final EventMetadata mockMetadata = mock(EventMetadata.class);
            when(mockEvent.getMetadata()).thenReturn(mockMetadata);
            when(mockMetadata.getEventType()).thenReturn("DOCUMENT");
            eventConsumer.accept(new Record<>(mockEvent));
            return null;
        }).when(mockCodec).parse(any(InputStream.class), any());

        bulkSqsMessageHandler.handleMessage(message, queueUrl, mockBuffer, bufferTimeoutMillis, null);
        ArgumentCaptor<Record<Event>> argumentCaptor = ArgumentCaptor.forClass(Record.class);
        verify(mockBuffer, times(1)).write(argumentCaptor.capture(), eq(bufferTimeoutMillis));
        Record<Event> capturedRecord = argumentCaptor.getValue();
        assertEquals(
                "DOCUMENT",
                capturedRecord.getData().getMetadata().getEventType(),
                "Event type should be 'DOCUMENT'"
        );
    }
}
