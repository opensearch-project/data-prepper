//package org.opensearch.dataprepper.plugins.lambda.processor;
//
//import io.micrometer.core.instrument.Counter;
//import io.micrometer.core.instrument.Timer;
//import static org.junit.jupiter.api.Assertions.assertEquals;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import static org.mockito.ArgumentMatchers.any;
//import static org.mockito.ArgumentMatchers.anyString;
//import org.mockito.Mock;
//import static org.mockito.Mockito.doNothing;
//import static org.mockito.Mockito.eq;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.never;
//import static org.mockito.Mockito.times;
//import static org.mockito.Mockito.verify;
//import static org.mockito.Mockito.when;
//import org.mockito.MockitoAnnotations;
//import org.mockito.junit.jupiter.MockitoSettings;
//import org.mockito.quality.Strictness;
//import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
//import org.opensearch.dataprepper.expression.ExpressionEvaluator;
//import org.opensearch.dataprepper.metrics.PluginMetrics;
//import org.opensearch.dataprepper.model.codec.OutputCodec;
//import org.opensearch.dataprepper.model.event.Event;
//import org.opensearch.dataprepper.model.plugin.PluginFactory;
//import org.opensearch.dataprepper.model.record.Record;
//import org.opensearch.dataprepper.model.types.ByteCount;
//import org.opensearch.dataprepper.plugins.codec.json.JsonInputCodec;
//import org.opensearch.dataprepper.plugins.lambda.common.LambdaCommonHandler;
//import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
//import org.opensearch.dataprepper.plugins.lambda.common.accumlator.BufferFactory;
//import org.opensearch.dataprepper.plugins.lambda.common.accumlator.InMemoryBuffer;
//import org.opensearch.dataprepper.plugins.lambda.common.config.AwsAuthenticationOptions;
//import org.opensearch.dataprepper.plugins.lambda.common.config.BatchOptions;
//import org.opensearch.dataprepper.plugins.lambda.common.config.InvocationType;
//import org.opensearch.dataprepper.plugins.lambda.common.config.ThresholdOptions;
//import software.amazon.awssdk.core.SdkBytes;
//import software.amazon.awssdk.regions.Region;
//import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
//import software.amazon.awssdk.services.lambda.model.InvokeRequest;
//import software.amazon.awssdk.services.lambda.model.InvokeResponse;
//
//import java.lang.reflect.Field;
//import java.time.Duration;
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.Collections;
//import java.util.List;
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.atomic.AtomicLong;
//
//@MockitoSettings(strictness = Strictness.LENIENT)
//public class LambdaProcessorTest {
//
//    @Mock
//    AwsAuthenticationOptions awsAuthenticationOptions;
//    @Mock
//    Buffer bufferMock;
//    @Mock
//    private PluginFactory pluginFactory;
//    @Mock
//    private PluginMetrics pluginMetrics;
//    @Mock
//    private LambdaProcessorConfig lambdaProcessorConfig;
//    @Mock
//    private AwsCredentialsSupplier awsCredentialsSupplier;
//    @Mock
//    private ExpressionEvaluator expressionEvaluator;
//    @Mock
//    private LambdaCommonHandler lambdaCommonHandler;
//    @Mock
//    private OutputCodec requestCodec;
//    @Mock
//    private JsonInputCodec responseCodec;
//    @Mock
//    private Counter numberOfRecordsSuccessCounter;
//    @Mock
//    private Counter numberOfRecordsFailedCounter;
//    @Mock
//    private InvokeResponse invokeResponse;
//
//    private LambdaProcessor lambdaProcessor;
//
//    @BeforeEach
//    public void setUp() throws Exception {
//        MockitoAnnotations.openMocks(this);
//
//        // Mock PluginMetrics counters and timers
//        when(pluginMetrics.counter(anyString())).thenReturn(numberOfRecordsSuccessCounter);
//        when(pluginMetrics.timer(anyString())).thenReturn(mock(Timer.class));
//        when(pluginMetrics.gauge(anyString(), any(AtomicLong.class))).thenReturn(new AtomicLong());
//
//        // Mock LambdaProcessorConfig
//        when(lambdaProcessorConfig.getFunctionName()).thenReturn("test-function");
//        when(lambdaProcessorConfig.getWhenCondition()).thenReturn(null);
//        when(lambdaProcessorConfig.getInvocationType()).thenReturn(InvocationType.REQUEST_RESPONSE);
//        when(lambdaProcessorConfig.getResponseEventsMatch()).thenReturn(false);
//
//        // Mock AWS Authentication Options
//        when(lambdaProcessorConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
//        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.US_EAST_1);
//        when(awsAuthenticationOptions.getAwsStsRoleArn()).thenReturn("testRole");
//
//        // Mock BatchOptions and ThresholdOptions
//        BatchOptions batchOptions = mock(BatchOptions.class);
//        ThresholdOptions thresholdOptions = mock(ThresholdOptions.class);
//        when(lambdaProcessorConfig.getBatchOptions()).thenReturn(batchOptions);
//        when(lambdaProcessorConfig.getConnectionTimeout()).thenReturn(Duration.ofSeconds(5));
//        when(batchOptions.getThresholdOptions()).thenReturn(thresholdOptions);
//        when(thresholdOptions.getEventCount()).thenReturn(10);
//        when(thresholdOptions.getMaximumSize()).thenReturn(ByteCount.parse("6mb"));
//        when(thresholdOptions.getEventCollectTimeOut()).thenReturn(Duration.ofSeconds(30));
//
//        // Initialize the LambdaProcessor with mocks
//        lambdaProcessor = new LambdaProcessor(pluginFactory, pluginMetrics, lambdaProcessorConfig, awsCredentialsSupplier, expressionEvaluator);
//
//        // Inject mocks into the LambdaProcessor using reflection
//        populatePrivateFields();
//        // Mock Buffer and LambdaCommonHandler
//        when(lambdaCommonHandler.createBuffer(any(BufferFactory.class))).thenReturn(bufferMock);
//        when(bufferMock.flushToLambda(anyString())).thenReturn(CompletableFuture.completedFuture(invokeResponse));
//
//        // Mock Request and Response Codecs
//        doNothing().when(requestCodec).start(any(), any(), any());
//        doNothing().when(requestCodec).writeEvent(any(), any());
//        doNothing().when(requestCodec).complete(any());
//
//        // Mock InvokeResponse
//        when(lambdaCommonHandler.checkStatusCode(any())).thenReturn(true);
//        when(invokeResponse.payload()).thenReturn(SdkBytes.fromUtf8String("[{\"key\":\"value\"}]"));
//
//        // Mock InvokeResponse
//        when(invokeResponse.payload()).thenReturn(SdkBytes.fromUtf8String("[{\"key\":\"value\"}]"));
//        when(invokeResponse.statusCode()).thenReturn(200); // Mock status code to be 200
//
//        // Mock LambdaAsyncClient
//        LambdaAsyncClient lambdaAsyncClientMock = mock(LambdaAsyncClient.class);
//        setPrivateField(lambdaProcessor, "lambdaAsyncClient", lambdaAsyncClientMock);
//
//        // Mock the invoke method
//        CompletableFuture<InvokeResponse> invokeFuture = CompletableFuture.completedFuture(invokeResponse);
//        when(lambdaAsyncClientMock.invoke(any(InvokeRequest.class))).thenReturn(invokeFuture);
//
//        // Mock LambdaCommonHandler
//        LambdaCommonHandler lambdaCommonHandler = mock(LambdaCommonHandler.class);
//        when(lambdaCommonHandler.checkStatusCode(any())).thenReturn(true);
//    }
//
//    private void populatePrivateFields() throws Exception {
//        List<String> tagsOnMatchFailure = Collections.singletonList("failure_tag");
//        // Use reflection to set the private fields
//        setPrivateField(lambdaProcessor, "requestCodec", requestCodec);
//        setPrivateField(lambdaProcessor, "responseCodec", responseCodec);
//        setPrivateField(lambdaProcessor, "futureList", new ArrayList<>());
//        setPrivateField(lambdaProcessor, "numberOfRecordsSuccessCounter", numberOfRecordsSuccessCounter);
//        setPrivateField(lambdaProcessor, "numberOfRecordsFailedCounter", numberOfRecordsFailedCounter);
//        setPrivateField(lambdaProcessor, "tagsOnMatchFailure", tagsOnMatchFailure);
//        setPrivateField(lambdaProcessor, "lambdaCommonHandler", lambdaCommonHandler);
//    }
//
//    // Helper method to set private fields via reflection
//    private void setPrivateField(Object targetObject, String fieldName, Object value) throws Exception {
//        Field field = targetObject.getClass().getDeclaredField(fieldName);
//        field.setAccessible(true);
//        field.set(targetObject, value);
//    }
//
//    private void setupTestObject() {
//        // Create the LambdaProcessor instance
//        lambdaProcessor = new LambdaProcessor(pluginFactory, pluginMetrics, lambdaProcessorConfig, awsCredentialsSupplier, expressionEvaluator);
//    }
//
//    @Test
//    public void testDoExecute_WithExceptionDuringProcessing() throws Exception {
//        // Arrange
//        Event event = mock(Event.class);
//        Record<Event> record = new Record<>(event);
//        List<Record<Event>> records = Collections.singletonList(record);
//
//        // Mock Buffer
//        Buffer bufferMock = mock(Buffer.class);
//        when(lambdaProcessor.lambdaCommonHandler.createBuffer(any(BufferFactory.class))).thenReturn(bufferMock);
//        when(bufferMock.getEventCount()).thenReturn(0, 1);
//        when(bufferMock.getRecords()).thenReturn(records);
//        doNothing().when(bufferMock).reset();
//
//        // Mock exception during flush
//        when(bufferMock.flushToLambda(any())).thenThrow(new RuntimeException("Test exception"));
//
//        // Act
//        Collection<Record<Event>> result = lambdaProcessor.doExecute(records);
//
////        // Wait for futures to complete
////        lambdaProcessor.lambdaCommonHandler.waitForFutures(lambdaProcessor.futureList);
//
//        // Assert
//        assertEquals(1, result.size());
//        verify(numberOfRecordsFailedCounter, times(1)).increment(1.0);
//    }
//
//    @Test
//    public void testDoExecute_WithEmptyResponse() throws Exception {
//        // Arrange
//        Event event = mock(Event.class);
//        Record<Event> record = new Record<>(event);
//        List<Record<Event>> records = Collections.singletonList(record);
//
//        // Mock Buffer
//        Buffer bufferMock = mock(Buffer.class);
//        when(lambdaProcessor.lambdaCommonHandler.createBuffer(any(BufferFactory.class))).thenReturn(bufferMock);
//        when(bufferMock.getEventCount()).thenReturn(0, 1);
//        when(bufferMock.getRecords()).thenReturn(records);
//        when(bufferMock.flushToLambda(any())).thenReturn(CompletableFuture.completedFuture(invokeResponse));
//        doNothing().when(bufferMock).reset();
//
//        when(invokeResponse.payload()).thenReturn(SdkBytes.fromUtf8String(""));
//
//        // Act
//        Collection<Record<Event>> result = lambdaProcessor.doExecute(records);
//
////        // Wait for futures to complete
////        lambdaProcessor.lambdaCommonHandler.waitForFutures(lambdaProcessor.futureList);
//
//        // Assert
//        assertEquals(0, result.size());
//        verify(numberOfRecordsSuccessCounter, times(1)).increment(1.0);
//    }
//
//    @Test
//    public void testDoExecute_WithNullResponse() throws Exception {
//        // Arrange
//        Event event = mock(Event.class);
//        Record<Event> record = new Record<>(event);
//        List<Record<Event>> records = Collections.singletonList(record);
//
//        // Mock Buffer
//        Buffer bufferMock = mock(Buffer.class);
//        when(lambdaProcessor.lambdaCommonHandler.createBuffer(any(BufferFactory.class))).thenReturn(bufferMock);
//        when(bufferMock.getEventCount()).thenReturn(0, 1);
//        when(bufferMock.getRecords()).thenReturn(records);
//        when(bufferMock.flushToLambda(any())).thenReturn(CompletableFuture.completedFuture(invokeResponse));
//        doNothing().when(bufferMock).reset();
//
//        when(invokeResponse.payload()).thenReturn(null);
//
//        // Act
//        Collection<Record<Event>> result = lambdaProcessor.doExecute(records);
//
//        // Wait for futures to complete
////        lambdaProcessor.lambdaCommonHandler.waitForFutures(lambdaProcessor.futureList);
//
//        // Assert
//        assertEquals(0, result.size());
//        verify(numberOfRecordsSuccessCounter, times(1)).increment(1.0);
//    }
//
//    @Test
//    public void testDoExecute_WithEmptyRecords() {
//        Collection<Record<Event>> records = Collections.emptyList();
//        Collection<Record<Event>> result = lambdaProcessor.doExecute(records);
//        assertEquals(0, result.size());
//    }
//
//    @Test
//    public void testDoExecute_WhenConditionFalse() {
//        Event event = mock(Event.class);
//        Record<Event> record = new Record<>(event);
//        Collection<Record<Event>> records = Collections.singletonList(record);
//        when(expressionEvaluator.evaluateConditional(anyString(), eq(event))).thenReturn(false);
//        when(lambdaProcessorConfig.getWhenCondition()).thenReturn("some_condition");
//        setupTestObject();
//
//        Collection<Record<Event>> result = lambdaProcessor.doExecute(records);
//
//        assertEquals(1, result.size());
//        verify(bufferMock, never()).flushToLambda(anyString());
//    }
//
//    @Test
//    public void testDoExecute_SuccessfulProcessing() throws Exception {
//        Event eventMock = mock(Event.class);
//        Record<Event> record = new Record<>(eventMock);
//        Collection<Record<Event>> records = Collections.singletonList(record);
//
//        when(bufferMock.getEventCount()).thenReturn(0).thenReturn(1);
//        when(bufferMock.getRecords()).thenReturn(Collections.singletonList(record));
//        doNothing().when(bufferMock).reset();
//
//        // Initialize futureList
//        setPrivateField(lambdaProcessor, "futureList", new ArrayList<>());
//
//        Collection<Record<Event>> result = lambdaProcessor.doExecute(records);
//
//        // Wait for futures to complete
////        lambdaProcessor.lambdaCommonHandler.waitForFutures(lambdaProcessor.futureList);
//
//        assertEquals(1, result.size(), "Result should contain one record.");
//        verify(numberOfRecordsSuccessCounter, times(1)).increment(1.0);
//        verify(requestCodec, times(1)).writeEvent(eq(eventMock), any());
//    }
//
//    @Test
//    public void testHandleFailure() {
//        Event event = mock(Event.class);
//        Buffer bufferMock = mock(Buffer.class);
//        List<Record<Event>> records = List.of(new Record<>(event));
//        when(bufferMock.getEventCount()).thenReturn(1);
//
//        lambdaProcessor.handleFailure(new RuntimeException("Test Exception"), bufferMock, records);
//
//        verify(numberOfRecordsFailedCounter, times(1)).increment(1);
//    }
//
//    @Test
//    public void testConvertLambdaResponseToEvent_WithEmptyPayload() throws Exception {
//        Event event = mock(Event.class);
//        Record<Event> record = new Record<>(event);
//        List<Record<Event>> records = Collections.singletonList(record);
//
//        InMemoryBuffer bufferMock = mock(InMemoryBuffer.class);
//        CompletableFuture<InvokeResponse> mockedFuture = CompletableFuture.completedFuture(invokeResponse);
//        when(bufferMock.flushToLambda(any())).thenReturn(mockedFuture);
//        when(lambdaCommonHandler.checkStatusCode(invokeResponse)).thenReturn(true);
//        when(invokeResponse.payload()).thenReturn(SdkBytes.fromUtf8String(""));
//
//        Collection<Record<Event>> result = lambdaProcessor.doExecute(records);
//        assertEquals(0, result.size());
//        verify(numberOfRecordsSuccessCounter, times(1)).increment(1.0);
//    }
//
////    @Test
////    public void testConvertLambdaResponseToEvent_WithEqualEventCounts_SuccessfulProcessing() throws Exception {
////        // Arrange
////        when(lambdaProcessorConfig.getResponseEventsMatch()).thenReturn(true);
////
////        // Mock LambdaResponse with a valid payload
////        String payloadString = "[{\"key\":\"value1\"}, {\"key\":\"value2\"}]";
////        SdkBytes sdkBytes = SdkBytes.fromByteArray(payloadString.getBytes());
////        when(invokeResponse.payload()).thenReturn(sdkBytes);
////        when(invokeResponse.statusCode()).thenReturn(200); // Success status code
////
////        // Mock the responseCodec.parse to add two events
////        doAnswer(invocation -> {
////            InputStream inputStream = invocation.getArgument(0);
////            @SuppressWarnings("unchecked") Consumer<Record<Event>> consumer = invocation.getArgument(1);
////            Event parsedEvent1 = mock(Event.class);
////            EventMetadata parsedEventMetadata1 = mock(EventMetadata.class);
////            when(parsedEvent1.getMetadata()).thenReturn(parsedEventMetadata1);
////
////            DefaultEventHandle eventHandle = mock(DefaultEventHandle.class);
////            AcknowledgementSet acknowledgementSet = mock(AcknowledgementSet.class);
////
////            when(parsedEvent1.getEventHandle()).thenReturn(eventHandle);
////            when(eventHandle.getAcknowledgementSet()).thenReturn(acknowledgementSet);
////
////            Event parsedEvent2 = mock(Event.class);
////            EventMetadata parsedEventMetadata2 = mock(EventMetadata.class);
////            when(parsedEvent2.getMetadata()).thenReturn(parsedEventMetadata2);
////            when(parsedEvent2.getEventHandle()).thenReturn(eventHandle);
////
////            consumer.accept(new Record<>(parsedEvent1));
////            consumer.accept(new Record<>(parsedEvent2));
////            return null;
////        }).when(responseCodec).parse(any(InputStream.class), any(Consumer.class));
////
////        // Mock buffer with two original events
////        Event originalEvent1 = mock(Event.class);
////        EventMetadata originalEventMetadata1 = mock(EventMetadata.class);
////        when(originalEvent1.getMetadata()).thenReturn(originalEventMetadata1);
////
////        Event originalEvent2 = mock(Event.class);
////        EventMetadata originalEventMetadata2 = mock(EventMetadata.class);
////        when(originalEvent2.getMetadata()).thenReturn(originalEventMetadata2);
////
////
////        DefaultEventHandle eventHandle = mock(DefaultEventHandle.class);
////        AcknowledgementSet acknowledgementSet = mock(AcknowledgementSet.class);
////        when(eventHandle.getAcknowledgementSet()).thenReturn(acknowledgementSet);
////
////        when(originalEvent1.getEventHandle()).thenReturn(eventHandle);
////        when(originalEvent2.getEventHandle()).thenReturn(eventHandle);
////
////
////        List<Record<Event>> originalRecords = Arrays.asList(new Record<>(originalEvent1), new Record<>(originalEvent2));
////
////        Buffer flushedBuffer = mock(Buffer.class);
////        when(flushedBuffer.getEventCount()).thenReturn(2);
////        when(flushedBuffer.getRecords()).thenReturn(originalRecords);
////
////        List<Record<Event>> resultRecords = new ArrayList<>();
////
////        // Act
////        lambdaProcessor.convertLambdaResponseToEvent(resultRecords, invokeResponse, flushedBuffer);
////
////        // Assert
////        assertNotNull(resultRecords);
////        assertEquals(2, resultRecords.size(), "ResultRecords should contain two records");
////
////    }
////
////    @Test
////    public void testConvertLambdaResponseToEvent_WithUnequalEventCounts_SuccessfulProcessing() throws Exception {
////        // Arrange
////        when(lambdaProcessorConfig.getResponseEventsMatch()).thenReturn(false);
////
////        // Mock LambdaResponse with a valid payload containing three events
////        String payloadString = "[{\"key\":\"value1\"}, {\"key\":\"value2\"}, {\"key\":\"value3\"}]";
////        SdkBytes sdkBytes = SdkBytes.fromByteArray(payloadString.getBytes());
////        when(invokeResponse.payload()).thenReturn(sdkBytes);
////        when(invokeResponse.statusCode()).thenReturn(200); // Success status code
////
////        // Mock the responseCodec.parse to add three events
////        doAnswer(invocation -> {
////            InputStream inputStream = invocation.getArgument(0);
////            Consumer<Record<Event>> consumer = invocation.getArgument(1);
////            Event parsedEvent1 = mock(Event.class);
////            Event parsedEvent2 = mock(Event.class);
////            Event parsedEvent3 = mock(Event.class);
////
////            consumer.accept(new Record<>(parsedEvent1));
////            consumer.accept(new Record<>(parsedEvent2));
////            consumer.accept(new Record<>(parsedEvent3));
////            return null;
////        }).when(responseCodec).parse(any(InputStream.class), any(Consumer.class));
////
////        // Mock buffer with two original events
////        Event originalEvent1 = mock(Event.class);
////        Event originalEvent2 = mock(Event.class);
////
////        DefaultEventHandle eventHandle = mock(DefaultEventHandle.class);
////        AcknowledgementSet acknowledgementSet = mock(AcknowledgementSet.class);
////        when(eventHandle.getAcknowledgementSet()).thenReturn(acknowledgementSet);
////
////        when(originalEvent1.getEventHandle()).thenReturn(eventHandle);
////        when(originalEvent2.getEventHandle()).thenReturn(eventHandle);
////
////        List<Record<Event>> originalRecords = Arrays.asList(new Record<>(originalEvent1), new Record<>(originalEvent2));
////
////        Buffer flushedBuffer = mock(Buffer.class);
////        when(flushedBuffer.getEventCount()).thenReturn(2);
////        when(flushedBuffer.getRecords()).thenReturn(originalRecords);
////
////        List<Record<Event>> resultRecords = new ArrayList<>();
////
////        // Act
////        lambdaProcessor.convertLambdaResponseToEvent(resultRecords, invokeResponse, flushedBuffer);
////
////        // Assert
////        assertNotNull(resultRecords);
////        assertEquals(3, resultRecords.size(), "ResultRecords should contain three records");
////
////        // Verify that original events were not cleared
////        verify(originalEvent1, never()).clear();
////        verify(originalEvent2, never()).clear();
////    }
//
//}


/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.processor;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import org.mockito.Captor;
import org.mockito.Mock;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.DefaultEventHandle;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.lambda.common.LambdaCommonHandler;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.BufferFactory;
import org.opensearch.dataprepper.plugins.lambda.common.config.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.BatchOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.InvocationType;
import org.opensearch.dataprepper.plugins.lambda.common.config.ThresholdOptions;
import static org.opensearch.dataprepper.plugins.lambda.processor.LambdaProcessor.NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_FAILED;
import static org.opensearch.dataprepper.plugins.lambda.processor.LambdaProcessor.NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_SUCCESS;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@MockitoSettings(strictness = Strictness.LENIENT)
public class LambdaProcessorTest {

    // Mock dependencies
    @Mock
    private AwsAuthenticationOptions awsAuthenticationOptions;

    @Mock
    private Buffer bufferMock;

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private LambdaProcessorConfig lambdaProcessorConfig;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    @Mock
    private LambdaCommonHandler lambdaCommonHandler;

    @Mock
    private InputCodec responseCodec;

    @Mock
    private OutputCodec requestCodec;

    @Mock
    private Counter numberOfRecordsSuccessCounter;

    @Mock
    private Counter numberOfRecordsFailedCounter;

    @Mock
    private InvokeResponse invokeResponse;

    @Mock
    private Timer lambdaLatencyMetric;

    @Captor
    private ArgumentCaptor<Consumer<Record<Event>>> consumerCaptor;

    // The class under test
    private LambdaProcessor lambdaProcessor;

    @BeforeEach
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        // Mock PluginMetrics counters and timers
        when(pluginMetrics.counter(eq(NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_SUCCESS))).thenReturn(numberOfRecordsSuccessCounter);
        when(pluginMetrics.counter(eq(NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_FAILED))).thenReturn(numberOfRecordsFailedCounter);
        when(pluginMetrics.timer(anyString())).thenReturn(lambdaLatencyMetric);
        when(pluginMetrics.gauge(anyString(), any(AtomicLong.class))).thenAnswer(invocation -> invocation.getArgument(1));

        // Mock LambdaProcessorConfig
        when(lambdaProcessorConfig.getFunctionName()).thenReturn("test-function");
        when(lambdaProcessorConfig.getWhenCondition()).thenReturn(null);
        when(lambdaProcessorConfig.getInvocationType()).thenReturn(InvocationType.REQUEST_RESPONSE);
        when(lambdaProcessorConfig.getResponseEventsMatch()).thenReturn(false);
        when(lambdaProcessorConfig.getMaxConnectionRetries()).thenReturn(3);
        when(lambdaProcessorConfig.getConnectionTimeout()).thenReturn(Duration.ofSeconds(5));

        // Mock AWS Authentication Options
        when(lambdaProcessorConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.US_EAST_1);
        when(awsAuthenticationOptions.getAwsStsRoleArn()).thenReturn("testRole");

        // Mock BatchOptions and ThresholdOptions
        BatchOptions batchOptions = mock(BatchOptions.class);
        ThresholdOptions thresholdOptions = mock(ThresholdOptions.class);
        when(lambdaProcessorConfig.getBatchOptions()).thenReturn(batchOptions);
        when(batchOptions.getThresholdOptions()).thenReturn(thresholdOptions);
        when(thresholdOptions.getEventCount()).thenReturn(10);
        when(thresholdOptions.getMaximumSize()).thenReturn(ByteCount.parse("6mb"));
        when(thresholdOptions.getEventCollectTimeOut()).thenReturn(Duration.ofSeconds(30));
        when(batchOptions.getKeyName()).thenReturn("key");

        // Mock Response Codec Configuration
        PluginModel responseCodecConfig = lambdaProcessorConfig.getResponseCodecConfig();
        PluginSetting responseCodecPluginSetting;

        if (responseCodecConfig == null) {
            // Default to JsonInputCodec with default settings
            responseCodecPluginSetting = new PluginSetting("json", Collections.emptyMap());
        } else {
            responseCodecPluginSetting = new PluginSetting(responseCodecConfig.getPluginName(), responseCodecConfig.getPluginSettings());
        }

        // Mock PluginFactory to return the mocked responseCodec
        when(pluginFactory.loadPlugin(eq(InputCodec.class), any(PluginSetting.class))).thenReturn(responseCodec);

        // Instantiate the LambdaProcessor manually
        lambdaProcessor = new LambdaProcessor(pluginFactory, pluginMetrics, lambdaProcessorConfig, awsCredentialsSupplier, expressionEvaluator);

        // Inject mocks into the LambdaProcessor using reflection
        populatePrivateFields();

        // Mock LambdaCommonHandler behavior
        when(lambdaCommonHandler.createBuffer(any(BufferFactory.class))).thenReturn(bufferMock);

        // Mock Buffer behavior for flushToLambda
        when(bufferMock.flushToLambda(anyString())).thenReturn(CompletableFuture.completedFuture(invokeResponse));

        // Mock InvokeResponse
        when(invokeResponse.payload()).thenReturn(SdkBytes.fromUtf8String("[{\"key\":\"value\"}]"));
        when(invokeResponse.statusCode()).thenReturn(200); // Success status code

        // Mock LambdaAsyncClient inside LambdaProcessor
        LambdaAsyncClient lambdaAsyncClientMock = mock(LambdaAsyncClient.class);
        setPrivateField(lambdaProcessor, "lambdaAsyncClient", lambdaAsyncClientMock);

        // Mock the invoke method to return a completed future
        CompletableFuture<InvokeResponse> invokeFuture = CompletableFuture.completedFuture(invokeResponse);
        when(lambdaAsyncClientMock.invoke(any(InvokeRequest.class))).thenReturn(invokeFuture);

        // Mock the checkStatusCode method
        when(lambdaCommonHandler.checkStatusCode(any())).thenReturn(true);

        // Mock Response Codec parse method
        doNothing().when(responseCodec).parse(any(InputStream.class), any(Consumer.class));

    }

    private void populatePrivateFields() throws Exception {
        List<String> tagsOnMatchFailure = Collections.singletonList("failure_tag");
        // Use reflection to set the private fields
        setPrivateField(lambdaProcessor, "numberOfRecordsSuccessCounter", numberOfRecordsSuccessCounter);
        setPrivateField(lambdaProcessor, "numberOfRecordsFailedCounter", numberOfRecordsFailedCounter);
        setPrivateField(lambdaProcessor, "tagsOnMatchFailure", tagsOnMatchFailure);
        setPrivateField(lambdaProcessor, "lambdaCommonHandler", lambdaCommonHandler);
    }

    // Helper method to set private fields via reflection
    private void setPrivateField(Object targetObject, String fieldName, Object value) throws Exception {
        Field field = targetObject.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(targetObject, value);
    }

    @Test
    public void testDoExecute_WithExceptionDuringProcessing() throws Exception {
        // Arrange
        Event event = mock(Event.class);
        Record<Event> record = new Record<>(event);
        List<Record<Event>> records = Collections.singletonList(record);

        // Mock Buffer
        Buffer bufferMock = mock(Buffer.class);
        when(lambdaProcessor.lambdaCommonHandler.createBuffer(any(BufferFactory.class))).thenReturn(bufferMock);
        when(bufferMock.getEventCount()).thenReturn(0, 1);
        when(bufferMock.getRecords()).thenReturn(records);
        doNothing().when(bufferMock).reset();

        // Mock exception during flush
        when(bufferMock.flushToLambda(any())).thenThrow(new RuntimeException("Test exception"));

        // Act
        Collection<Record<Event>> result = lambdaProcessor.doExecute(records);

        // Assert
        assertEquals(1, result.size());
        verify(numberOfRecordsFailedCounter, times(1)).increment(1.0);
    }

    @Test
    public void testDoExecute_WithEmptyResponse() throws Exception {
        // Arrange
        Event event = mock(Event.class);
        Record<Event> record = new Record<>(event);
        List<Record<Event>> records = Collections.singletonList(record);

        // Mock Buffer to return empty payload
        when(invokeResponse.payload()).thenReturn(SdkBytes.fromUtf8String(""));

        // Act
        Collection<Record<Event>> result = lambdaProcessor.doExecute(records);

        // Assert
        assertEquals(0, result.size(), "Result should be empty due to empty Lambda response.");
        verify(numberOfRecordsSuccessCounter, times(1)).increment(1.0);
    }

    @Test
    public void testDoExecute_WithNullResponse() throws Exception {
        // Arrange
        Event event = mock(Event.class);
        Record<Event> record = new Record<>(event);
        List<Record<Event>> records = Collections.singletonList(record);

        // Mock Buffer to return null payload
        when(invokeResponse.payload()).thenReturn(null);

        // Act
        Collection<Record<Event>> result = lambdaProcessor.doExecute(records);

        // Assert
        assertEquals(0, result.size(), "Result should be empty due to null Lambda response.");
        verify(numberOfRecordsSuccessCounter, times(1)).increment(1.0);
    }

    @Test
    public void testDoExecute_WithEmptyRecords() {
        // Arrange
        Collection<Record<Event>> records = Collections.emptyList();

        // Act
        Collection<Record<Event>> result = lambdaProcessor.doExecute(records);

        // Assert
        assertEquals(0, result.size(), "Result should be empty when input records are empty.");
        verify(numberOfRecordsSuccessCounter, never()).increment(anyDouble());
        verify(numberOfRecordsFailedCounter, never()).increment(anyDouble());
    }

    @Test
    public void testDoExecute_WhenConditionFalse() {
        // Arrange
        Event event = mock(Event.class);
        DefaultEventHandle eventHandle = mock(DefaultEventHandle.class);
        AcknowledgementSet acknowledgementSet = mock(AcknowledgementSet.class);
        when(event.getEventHandle()).thenReturn(eventHandle);
        when(eventHandle.getAcknowledgementSet()).thenReturn(acknowledgementSet);
        Record<Event> record = new Record<>(event);
        Collection<Record<Event>> records = Collections.singletonList(record);

        // Mock condition evaluator to return false
        when(expressionEvaluator.evaluateConditional(anyString(), eq(event))).thenReturn(false);
        when(lambdaProcessorConfig.getWhenCondition()).thenReturn("some_condition");

        // Instantiate the LambdaProcessor manually
        lambdaProcessor = new LambdaProcessor(pluginFactory, pluginMetrics, lambdaProcessorConfig, awsCredentialsSupplier, expressionEvaluator);

        // Act
        Collection<Record<Event>> result = lambdaProcessor.doExecute(records);

        // Assert
        assertEquals(1, result.size(), "Result should contain one record as the condition is false.");
        verify(lambdaCommonHandler, never()).createBuffer(any(BufferFactory.class));
        verify(bufferMock, never()).flushToLambda(anyString());
        verify(numberOfRecordsSuccessCounter, never()).increment(anyDouble());
        verify(numberOfRecordsFailedCounter, never()).increment(anyDouble());
    }

    @Test
    public void testDoExecute_SuccessfulProcessing() throws Exception {
        // Arrange
        Event event = mock(Event.class);
        DefaultEventHandle eventHandle = mock(DefaultEventHandle.class);
        AcknowledgementSet acknowledgementSet = mock(AcknowledgementSet.class);
        when(event.getEventHandle()).thenReturn(eventHandle);
        when(eventHandle.getAcknowledgementSet()).thenReturn(acknowledgementSet);
        Record<Event> record = new Record<>(event);
        Collection<Record<Event>> records = Collections.singletonList(record);

        // Mock LambdaAsyncClient inside LambdaProcessor
        LambdaAsyncClient lambdaAsyncClientMock = mock(LambdaAsyncClient.class);
        setPrivateField(lambdaProcessor, "lambdaAsyncClient", lambdaAsyncClientMock);

        // Mock the invoke method to return a completed future
        CompletableFuture<InvokeResponse> invokeFuture = CompletableFuture.completedFuture(invokeResponse);
        when(lambdaAsyncClientMock.invoke(any(InvokeRequest.class))).thenReturn(invokeFuture);


        // Mock Buffer behavior
        when(bufferMock.getEventCount()).thenReturn(0).thenReturn(1).thenReturn(0);
        when(bufferMock.getRecords()).thenReturn(Collections.singletonList(record));
        doNothing().when(bufferMock).reset();

        doAnswer(invocation -> {
            InputStream inputStream = invocation.getArgument(0);
            @SuppressWarnings("unchecked")
            Consumer<Record<Event>> consumer = invocation.getArgument(1);

            // Simulate parsing by providing a mocked event
            Event parsedEvent = mock(Event.class);
            Record<Event> parsedRecord = new Record<>(parsedEvent);
            consumer.accept(parsedRecord);

            return null;
        }).when(responseCodec).parse(any(InputStream.class), any(Consumer.class));

        // Act
        Collection<Record<Event>> result = lambdaProcessor.doExecute(records);

        // Assert
        assertEquals(1, result.size(), "Result should contain one record.");
        verify(numberOfRecordsSuccessCounter, times(1)).increment(1.0);
    };


    @Test
    public void testHandleFailure() {
        // Arrange
        Event event = mock(Event.class);
        Buffer bufferMock = mock(Buffer.class);
        List<Record<Event>> records = List.of(new Record<>(event));
        when(bufferMock.getEventCount()).thenReturn(1);
        when(bufferMock.getRecords()).thenReturn(records);

        // Act
        lambdaProcessor.handleFailure(new RuntimeException("Test Exception"), bufferMock, records);

        // Assert
        verify(numberOfRecordsFailedCounter, times(1)).increment(1.0);
        // Ensure failure tags are added; assuming addFailureTags is implemented correctly
        // You might need to verify interactions with event metadata if it's mocked
    }

    @Test
    public void testConvertLambdaResponseToEvent_WithEqualEventCounts_SuccessfulProcessing() throws Exception {
        // Arrange
        when(lambdaProcessorConfig.getResponseEventsMatch()).thenReturn(true);

        // Mock LambdaResponse with a valid payload containing two events
        String payloadString = "[{\"key\":\"value1\"}, {\"key\":\"value2\"}]";
        SdkBytes sdkBytes = SdkBytes.fromByteArray(payloadString.getBytes());
        when(invokeResponse.payload()).thenReturn(sdkBytes);
        when(invokeResponse.statusCode()).thenReturn(200); // Success status code

        // Mock the responseCodec.parse to add two events
        doAnswer(invocation -> {
            InputStream inputStream = invocation.getArgument(0);
            @SuppressWarnings("unchecked")
            Consumer<Record<Event>> consumer = invocation.getArgument(1);
            Event parsedEvent1 = mock(Event.class);
            Event parsedEvent2 = mock(Event.class);
            consumer.accept(new Record<>(parsedEvent1));
            consumer.accept(new Record<>(parsedEvent2));
            return null;
        }).when(responseCodec).parse(any(InputStream.class), any(Consumer.class));

        // Mock buffer with two original events
        Event originalEvent1 = mock(Event.class);
        Event originalEvent2 = mock(Event.class);
        DefaultEventHandle eventHandle = mock(DefaultEventHandle.class);
        AcknowledgementSet acknowledgementSet = mock(AcknowledgementSet.class);
        when(eventHandle.getAcknowledgementSet()).thenReturn(acknowledgementSet);

        when(originalEvent1.getEventHandle()).thenReturn(eventHandle);
        when(originalEvent2.getEventHandle()).thenReturn(eventHandle);
        Record<Event> originalRecord1 = new Record<>(originalEvent1);
        Record<Event> originalRecord2 = new Record<>(originalEvent2);
        List<Record<Event>> originalRecords = Arrays.asList(originalRecord1, originalRecord2);
        when(bufferMock.getRecords()).thenReturn(originalRecords);
        when(bufferMock.getEventCount()).thenReturn(2);

        // Act
        List<Record<Event>> resultRecords = new ArrayList<>();
        lambdaProcessor.convertLambdaResponseToEvent(resultRecords, invokeResponse, bufferMock, responseCodec);

        // Assert
        assertEquals(2, resultRecords.size(), "ResultRecords should contain two records.");
        // Verify that failure tags are not added since it's a successful response
        verify(originalEvent1, never()).getMetadata();
        verify(originalEvent2, never()).getMetadata();
    }

    @Test
    public void testConvertLambdaResponseToEvent_WithUnequalEventCounts_SuccessfulProcessing() throws Exception {
        // Arrange
        // Set responseEventsMatch to false
        when(lambdaProcessorConfig.getResponseEventsMatch()).thenReturn(false);

        // Mock LambdaResponse with a valid payload containing three events
        String payloadString = "[{\"key\":\"value1\"}, {\"key\":\"value2\"}, {\"key\":\"value3\"}]";
        SdkBytes sdkBytes = SdkBytes.fromByteArray(payloadString.getBytes());
        when(invokeResponse.payload()).thenReturn(sdkBytes);
        when(invokeResponse.statusCode()).thenReturn(200); // Success status code

        // Mock the responseCodec.parse to add three parsed events
        doAnswer(invocation -> {
            InputStream inputStream = invocation.getArgument(0);
            @SuppressWarnings("unchecked")
            Consumer<Record<Event>> consumer = invocation.getArgument(1);

            // Create and add three mocked parsed events
            Event parsedEvent1 = mock(Event.class);
            Event parsedEvent2 = mock(Event.class);
            Event parsedEvent3 = mock(Event.class);
            consumer.accept(new Record<>(parsedEvent1));
            consumer.accept(new Record<>(parsedEvent2));
            consumer.accept(new Record<>(parsedEvent3));

            return null;
        }).when(responseCodec).parse(any(InputStream.class), any(Consumer.class));

        // Mock buffer with two original events
        Event originalEvent1 = mock(Event.class);
        EventMetadata originalMetadata1 = mock(EventMetadata.class);
        when(originalEvent1.getMetadata()).thenReturn(originalMetadata1);

        Event originalEvent2 = mock(Event.class);
        EventMetadata originalMetadata2 = mock(EventMetadata.class);
        when(originalEvent2.getMetadata()).thenReturn(originalMetadata2);

        DefaultEventHandle eventHandle = mock(DefaultEventHandle.class);
        AcknowledgementSet acknowledgementSet = mock(AcknowledgementSet.class);
        when(eventHandle.getAcknowledgementSet()).thenReturn(acknowledgementSet);

        when(originalEvent1.getEventHandle()).thenReturn(eventHandle);
        when(originalEvent2.getEventHandle()).thenReturn(eventHandle);

        Record<Event> originalRecord1 = new Record<>(originalEvent1);
        Record<Event> originalRecord2 = new Record<>(originalEvent2);
        List<Record<Event>> originalRecords = Arrays.asList(originalRecord1, originalRecord2);
        when(bufferMock.getRecords()).thenReturn(originalRecords);
        when(bufferMock.getEventCount()).thenReturn(2);

        // Act
        List<Record<Event>> resultRecords = new ArrayList<>();
        lambdaProcessor.convertLambdaResponseToEvent(resultRecords, invokeResponse, bufferMock, responseCodec);

        // Assert
        // Verify that three records are added to the result
        assertEquals(3, resultRecords.size(), "ResultRecords should contain three records.");
    }

}