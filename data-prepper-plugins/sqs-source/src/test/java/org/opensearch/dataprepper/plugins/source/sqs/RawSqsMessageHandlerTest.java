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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import software.amazon.awssdk.services.sqs.model.Message;
import org.opensearch.dataprepper.model.codec.InputCodec;

import java.io.InputStream;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

class RawSqsMessageHandlerTest {

    private Buffer<Record<Event>> mockBuffer;
    private int mockBufferTimeoutMillis;

    @BeforeEach
    void setUp() {
        mockBuffer = Mockito.mock(Buffer.class);
        mockBufferTimeoutMillis = 10000;
    }

    @Test
    void handleMessage_standardStrategy_callsBufferWriteAllOnce() throws Exception {
        MessageFieldStrategy standardMessageFieldStrategy = new StandardMessageFieldStrategy();
        RawSqsMessageHandler handler = new RawSqsMessageHandler(standardMessageFieldStrategy);
        Message message = Message.builder()
                .body("{\"key\":\"value\"}")
                .build();
        String queueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue";
        handler.handleMessage(message, queueUrl, mockBuffer, mockBufferTimeoutMillis, null);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<Record<Event>>> argumentCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(mockBuffer, Mockito.times(1)).writeAll(argumentCaptor.capture(), eq(mockBufferTimeoutMillis));
        List<Record<Event>> capturedRecords = argumentCaptor.getValue();
        assertEquals(1, capturedRecords.size(), "Raw strategy should produce exactly one record");
        assertEquals("DOCUMENT", capturedRecords.get(0).getData().getMetadata().getEventType(),
                "Event type should be 'DOCUMENT'");
    }

    @Test
    void handleMessage_bulkStrategy_callsBufferWriteAllWithMultipleEvents() throws Exception {
        InputCodec mockCodec = Mockito.mock(InputCodec.class);
        Mockito.doAnswer(invocation -> {
            InputStream inputStream = invocation.getArgument(0);
            @SuppressWarnings("unchecked")
            Consumer<Record<Event>> eventConsumer = invocation.getArgument(1);
            Event event1 = JacksonEvent.builder()
                    .withEventType("DOCUMENT")
                    .withData(singletonMap("key1", "val1"))
                    .build();
            Event event2 = JacksonEvent.builder()
                    .withEventType("DOCUMENT")
                    .withData(singletonMap("key2", "val2"))
                    .build();
            eventConsumer.accept(new Record<>(event1));
            eventConsumer.accept(new Record<>(event2));
            return null;
        }).when(mockCodec).parse(any(InputStream.class), any());
        MessageFieldStrategy bulkStrategy = new JsonBulkMessageFieldStrategy(mockCodec);
        RawSqsMessageHandler handler = new RawSqsMessageHandler(bulkStrategy);
        String messageBody = "{\"events\":[{\"foo\":\"bar1\"},{\"foo\":\"bar2\"}]}";
        Message message = Message.builder()
                .body(messageBody)
                .build();
        String queueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue";
        handler.handleMessage(message, queueUrl, mockBuffer, mockBufferTimeoutMillis, null);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<Record<Event>>> argumentCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(mockBuffer, Mockito.times(1)).writeAll(argumentCaptor.capture(), eq(mockBufferTimeoutMillis));

        List<Record<Event>> capturedRecords = argumentCaptor.getValue();
        assertEquals(2, capturedRecords.size(), "Bulk strategy should produce two records");
        for (Record<Event> record : capturedRecords) {
            assertEquals("DOCUMENT", record.getData().getMetadata().getEventType(),
                    "Event type should be 'DOCUMENT'");
        }
    }
}
