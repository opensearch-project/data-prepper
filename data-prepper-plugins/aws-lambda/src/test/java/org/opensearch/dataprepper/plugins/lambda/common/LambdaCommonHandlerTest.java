package org.opensearch.dataprepper.plugins.lambda.common;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.config.InvocationType;
import org.slf4j.Logger;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LambdaCommonHandlerTest {

    private final String functionName = "test-function";
    private final String invocationType = InvocationType.REQUEST_RESPONSE.getAwsLambdaValue();
    @Mock
    private Logger mockLogger;
    @Mock
    private LambdaAsyncClient mockLambdaAsyncClient;
    
    @Mock
    private Buffer mockBuffer;
    @Mock
    private InvokeResponse mockInvokeResponse;
    @InjectMocks
    private LambdaCommonHandler lambdaCommonHandler;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testCreateBuffer_success() throws IOException {
        // Arrange
        when(mockBufferFactory.getBuffer(any(), anyString(), any())).thenReturn(mockBuffer);

        // Act
        Buffer result = lambdaCommonHandler.createBuffer(mockBufferFactory);

        // Assert
        verify(mockBufferFactory, times(1)).getBuffer(mockLambdaAsyncClient, functionName, invocationType);
        verify(mockLogger, times(1)).debug("Resetting buffer");
        assertEquals(result, mockBuffer);
    }

    @Test
    public void testCreateBuffer_throwsException() throws IOException {
        // Arrange
        when(mockBufferFactory.getBuffer(any(), anyString(), any())).thenThrow(new IOException("Test Exception"));

        // Act & Assert
        try {
            lambdaCommonHandler.createBuffer(mockBufferFactory);
        } catch (RuntimeException e) {
            assert e.getMessage().contains("Failed to reset buffer");
        }
        verify(mockBufferFactory, times(1)).getBuffer(mockLambdaAsyncClient, functionName, invocationType);
    }

    @Test
    public void testWaitForFutures_allComplete() {
        // Arrange
        List<CompletableFuture<Void>> futureList = new ArrayList<>();
        futureList.add(CompletableFuture.completedFuture(null));
        futureList.add(CompletableFuture.completedFuture(null));

        // Act
        LambdaCommonHandler.waitForFutures(futureList);

        // Assert
        assert futureList.isEmpty();
    }

    @Test
    public void testWaitForFutures_withException() {
        // Arrange
        List<CompletableFuture<Void>> futureList = new ArrayList<>();
        futureList.add(CompletableFuture.failedFuture(new RuntimeException("Test Exception")));

        // Act
        LambdaCommonHandler.waitForFutures(futureList);

        // Assert
        assert futureList.isEmpty();
    }

    private List<EventHandle> mockEventHandleList(int size) {
        List<EventHandle> eventHandleList = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            EventHandle eventHandle = mock(EventHandle.class);
            eventHandleList.add(eventHandle);
        }
        return eventHandleList;
    }

}
