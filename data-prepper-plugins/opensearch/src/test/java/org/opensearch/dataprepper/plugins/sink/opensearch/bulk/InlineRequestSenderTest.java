package org.opensearch.dataprepper.plugins.sink.opensearch.bulk;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class InlineRequestSenderTest {
    @Mock
    private AccumulatingBulkRequest request;
    @Mock
    private Consumer<AccumulatingBulkRequest> requestConsumer;

    private InlineRequestSender inlineRequestSender;

    @BeforeEach
    void setup() {
        MockitoAnnotations.openMocks(this);
        inlineRequestSender = new InlineRequestSender();
    }

    @AfterEach
    void tearDown() {
        verifyNoMoreInteractions(request, requestConsumer);
    }

    @Test
    void sendRequest_Success() {
        inlineRequestSender.sendRequest(requestConsumer, request);

        verify(requestConsumer).accept(request);
    }

    @Test
    void sendRequest_BubblesUpException() {
        doThrow(new RuntimeException()).when(requestConsumer).accept(request);

        assertThrows(RuntimeException.class, () -> inlineRequestSender.sendRequest(requestConsumer, request));

        verify(requestConsumer).accept(request);
    }
}
