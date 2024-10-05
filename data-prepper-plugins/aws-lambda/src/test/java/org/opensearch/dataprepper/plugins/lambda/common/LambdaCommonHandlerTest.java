package org.opensearch.dataprepper.plugins.lambda.common;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import org.mockito.Mock;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.MockitoAnnotations;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.BufferFactory;
import org.opensearch.dataprepper.plugins.lambda.sink.dlq.DlqPushHandler;
import org.opensearch.dataprepper.plugins.lambda.sink.dlq.LambdaSinkFailedDlqData;
import org.slf4j.Logger;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class LambdaCommonHandlerTest {

    @Mock
    private Logger LOG;

    @Mock
    private LambdaAsyncClient lambdaAsyncClient;

    @Mock
    private Buffer currentBuffer;

    @Mock
    private BufferFactory bufferFactory;

    @Mock
    private OutputCodec codec;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    @Mock
    private Counter numberOfRecordsSuccessCounter;

    @Mock
    private Counter numberOfRecordsFailedCounter;

    @Mock
    private Timer lambdaLatencyMetric;

    @Mock
    private DlqPushHandler dlqPushHandler;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private PluginSetting pluginSetting;

    @Mock
    private OutputCodecContext codecContext;
    
    private ByteCount maxBytes;  // ByteCount instance


    private LambdaCommonHandler lambdaCommonHandler;

    private static final String FUNCTION_NAME = "test-function";
    private static final String INVOCATION_TYPE = "RequestResponse";
    private static final int MAX_EVENTS = 4;
    private static final ByteCount MAX_BYTES = ByteCount.parse("5mb");
    private static final Duration MAX_COLLECTION_DURATION = Duration.ofSeconds(5);


    public void setupProcessorTestObject() throws IOException {
        MockitoAnnotations.openMocks(this);
        // Mock the bufferFactory to return the currentBuffer
        when(bufferFactory.getBuffer(any(), anyString(), anyString())).thenReturn(currentBuffer);

        lambdaCommonHandler = new LambdaCommonHandler(
                LOG,
                lambdaAsyncClient,
                FUNCTION_NAME,
                INVOCATION_TYPE,
                expressionEvaluator,
                numberOfRecordsSuccessCounter,
                numberOfRecordsFailedCounter,
                lambdaLatencyMetric,
                bufferFactory,
                codec,
                codecContext,
                true,
                null,
                MAX_EVENTS,
                MAX_BYTES,
                MAX_COLLECTION_DURATION,
                false,
                dlqPushHandler,
                pluginSetting);

        // Set up a completed future
        CompletableFuture<InvokeResponse> mockFuture = CompletableFuture.completedFuture(mock(InvokeResponse.class));
        // Mock flushToLambdaAsync to return the completed future
        when(currentBuffer.flushToLambdaAsync(anyString())).thenReturn(mockFuture);

    }

    public void setUpSinkTestObject() throws IOException {
        MockitoAnnotations.openMocks(this);
        // Mock the bufferFactory to return the currentBuffer
        when(bufferFactory.getBuffer(any(), anyString(), anyString())).thenReturn(currentBuffer);

        Boolean isSink = true;
        Boolean isBatchEnabled = true;
        lambdaCommonHandler = new LambdaCommonHandler(
                LOG,
                lambdaAsyncClient,
                FUNCTION_NAME,
                INVOCATION_TYPE,
                expressionEvaluator,
                numberOfRecordsSuccessCounter,
                numberOfRecordsFailedCounter,
                lambdaLatencyMetric,
                bufferFactory,
                codec,
                codecContext,
                isBatchEnabled,
                null,
                MAX_EVENTS,
                MAX_BYTES,
                MAX_COLLECTION_DURATION,
                isSink,
                dlqPushHandler,
                pluginSetting);


    }

    //Positive testcases

    @Test
    public void testProcessEvent_whenBufferNotFull() throws Exception {
        setupProcessorTestObject();
        when(currentBuffer.getEventCount()).thenReturn(0);
        List<Record<Event>> resultRecords = new ArrayList<>();
        Event mockEvent = mock(Event.class);

        lambdaCommonHandler.processEvent(resultRecords, mockEvent);

        // Verify
        verify(codec).start(any(), eq(mockEvent), any());
        verify(codec).writeEvent(eq(mockEvent), any());
        verify(currentBuffer).setEventCount(1);
        verify(lambdaAsyncClient, never()).invoke(any(InvokeRequest.class));
    }

    @Test
    public void testFlushToLambda_whenThresholdReached() throws Exception {
        setupProcessorTestObject();

        when(currentBuffer.getEventCount()).thenReturn(MAX_EVENTS);
        List<Record<Event>> resultRecords = new ArrayList<>();

        lambdaCommonHandler.flushToLambdaIfNeeded(resultRecords, false);

        verify(codec).complete(any());
        verify(currentBuffer).flushToLambdaAsync(eq(INVOCATION_TYPE));
    }

    @Test
    public void testForceFlush() throws Exception {
        setupProcessorTestObject();
        when(currentBuffer.getEventCount()).thenReturn(2);
        List<Record<Event>> resultRecords = new ArrayList<>();

        lambdaCommonHandler.flushToLambdaIfNeeded(resultRecords, true);

        verify(codec).complete(any());
        verify(currentBuffer).flushToLambdaAsync(eq(INVOCATION_TYPE));
    }

    @Test
    public void testHandleLambdaResponseSuccess() throws IOException {
        setupProcessorTestObject();
        InvokeResponse mockResponse = mock(InvokeResponse.class);
        when(mockResponse.statusCode()).thenReturn(200);

        lambdaCommonHandler.handleLambdaResponse(mockResponse);

        verify(LOG, never()).warn(anyString(), anyInt());
    }

    @Test
    public void testConvertLambdaResponseToEvent() throws Exception {
        setupProcessorTestObject();
        InvokeResponse mockResponse = mock(InvokeResponse.class);
        String mockPayload = "{\"key\":\"value\"}";
        var sdkBytes = SdkBytes.fromUtf8String(mockPayload);
        when(mockResponse.payload()).thenReturn(sdkBytes);

        JsonNode mockJsonNode = mock(JsonNode.class);
        when(objectMapper.readTree(anyString())).thenReturn(mockJsonNode);

        Event event = lambdaCommonHandler.convertLambdaResponseToEvent(mockResponse);
        assertNotNull(event);
    }

    //Negative Test Cases
    @Test
    public void testProcessEvent_failureHandling() throws Exception {
        setupProcessorTestObject();
        Event mockEvent = mock(Event.class);
        List<Record<Event>> resultRecords = new ArrayList<>();
        doThrow(IOException.class).when(codec).writeEvent(any(), any());

        assertThrows(IOException.class, () -> {
            lambdaCommonHandler.processEvent(resultRecords, mockEvent);
        });
    }

    @Test
    public void testResetBufferFailure() throws Exception {
        setupProcessorTestObject();
        doThrow(IOException.class).when(bufferFactory).getBuffer(any(), any(), any());

        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            lambdaCommonHandler.resetBuffer();
        });

        assertTrue(exception.getMessage().contains("Failed to reset buffer"));
    }

    @Test
    public void testThresholdCheckNotMet() throws Exception {
        setupProcessorTestObject();
        when(currentBuffer.getEventCount()).thenReturn(2);  // Below threshold
        List<Record<Event>> resultRecords = new ArrayList<>();

        lambdaCommonHandler.flushToLambdaIfNeeded(resultRecords, false);

        verify(codec, never()).complete(any());
        verify(lambdaAsyncClient, never()).invoke(any(InvokeRequest.class));
    }

    @Test
    public void testConvertLambdaResponseToEvent_invalidJsonPayload() throws Exception {
        setupProcessorTestObject();
        InvokeResponse mockResponse = mock(InvokeResponse.class);
        String invalidPayload = "invalid-json";
        SdkBytes sdkBytes = SdkBytes.fromUtf8String(invalidPayload);
        when(mockResponse.payload()).thenReturn(sdkBytes);

        when(objectMapper.readTree(anyString())).thenThrow(JsonParseException.class);

        assertThrows(RuntimeException.class, () -> {
            lambdaCommonHandler.convertLambdaResponseToEvent(mockResponse);
        });
    }

    @Test
    public void testHandleFailure_withDlq_Sink() throws IOException {
        setUpSinkTestObject();
        Throwable mockThrowable = new RuntimeException("Lambda failure");
        when(currentBuffer.getPayload()).thenReturn(SdkBytes.fromUtf8String("test-payload"));
        when(currentBuffer.getEventCount()).thenReturn(2);
        lambdaCommonHandler.handleFailure(mockThrowable);

        verify(dlqPushHandler).perform(eq(pluginSetting), any(LambdaSinkFailedDlqData.class));
        verify(numberOfRecordsFailedCounter).increment(2);
    }

    @Test
    public void testHandleFailure_withoutDlq() throws IOException {
        MockitoAnnotations.openMocks(this);
        when(bufferFactory.getBuffer(any(), anyString(), anyString())).thenReturn(currentBuffer);
        // Set dlqPushHandler to null
        lambdaCommonHandler = new LambdaCommonHandler(
                LOG,
                lambdaAsyncClient,
                FUNCTION_NAME,
                INVOCATION_TYPE,
                expressionEvaluator,
                numberOfRecordsSuccessCounter,
                numberOfRecordsFailedCounter,
                lambdaLatencyMetric,
                bufferFactory,
                codec,
                codecContext,
                true,
                null,
                MAX_EVENTS,
                MAX_BYTES,
                MAX_COLLECTION_DURATION,
                true,
                null,  // dlqPushHandler is null
                pluginSetting);

        when(currentBuffer.getEventCount()).thenReturn(2);
        Throwable mockThrowable = new RuntimeException("Lambda failure");
        when(currentBuffer.getPayload()).thenReturn(SdkBytes.fromUtf8String("test-payload"));

        lambdaCommonHandler.handleFailure(mockThrowable);

        verify(dlqPushHandler, never()).perform(any(), any());
        verify(numberOfRecordsFailedCounter).increment(2);
    }

}
