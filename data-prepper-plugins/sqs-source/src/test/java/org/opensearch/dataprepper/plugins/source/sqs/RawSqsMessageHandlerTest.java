package org.opensearch.dataprepper.plugins.source.sqs;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class RawSqsMessageHandlerTest {

    private final RawSqsMessageHandler rawSqsMessageHandler = new RawSqsMessageHandler();
    private BufferAccumulator<Record<Event>> mockBufferAccumulator;

    @BeforeEach
    void setUp() {
        mockBufferAccumulator = mock(BufferAccumulator.class);
    }

    @Test
    void parseMessageBody_validJsonString_returnsJsonNode() {
        String validJson = "{\"key\":\"value\"}";
        JsonNode result = rawSqsMessageHandler.parseMessageBody(validJson);
        assertTrue(result.isObject(), "Result should be a JSON object");
        assertEquals("value", result.get("key").asText(), "The value of 'key' should be 'value'");
    }

    @Test
    void handleMessage_callsBufferAccumulatorAddOnce() throws Exception {
        Message message = Message.builder().body("{\"key\":\"value\"}").build();
        String queueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue";
        rawSqsMessageHandler.handleMessage(message, queueUrl, mockBufferAccumulator, null);
        ArgumentCaptor<Record<Event>> argumentCaptor = ArgumentCaptor.forClass(Record.class);
        verify(mockBufferAccumulator, times(1)).add(argumentCaptor.capture());
        Record<Event> capturedRecord = argumentCaptor.getValue();
        assertEquals("sqs-event", capturedRecord.getData().getMetadata().getEventType(), "Event type should be 'sqs-event'");
    }

    @Test
    void handleMessage_handlesInvalidJsonBodyGracefully() throws Exception {
        String invalidJsonBody = "Invalid JSON string";
        Message message = Message.builder().body(invalidJsonBody).build();
        String queueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue";
        rawSqsMessageHandler.handleMessage(message, queueUrl, mockBufferAccumulator, null);
        ArgumentCaptor<Record<Event>> argumentCaptor = ArgumentCaptor.forClass(Record.class);
        verify(mockBufferAccumulator, times(1)).add(argumentCaptor.capture());
        Record<Event> capturedRecord = argumentCaptor.getValue();
        Map<String, Object> eventData = capturedRecord.getData().toMap();
        assertEquals(invalidJsonBody, eventData.get("message"), "The message should be a text node containing the original string");
    }

}
