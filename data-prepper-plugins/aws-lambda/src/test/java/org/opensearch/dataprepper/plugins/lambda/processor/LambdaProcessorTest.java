/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.opensearch.dataprepper.plugins.lambda.common.config.ClientOptions;
import static org.opensearch.dataprepper.plugins.lambda.processor.LambdaProcessor.NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_FAILED;
import static org.opensearch.dataprepper.plugins.lambda.processor.LambdaProcessor.NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_SUCCESS;
import static org.opensearch.dataprepper.plugins.lambda.processor.LambdaProcessor.NUMBER_OF_SUCCESSFUL_REQUESTS_TO_LAMBDA;
import static org.opensearch.dataprepper.plugins.lambda.processor.LambdaProcessor.NUMBER_OF_FAILED_REQUESTS_TO_LAMBDA;


import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
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
import org.opensearch.dataprepper.plugins.lambda.common.config.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.BatchOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.InvocationType;
import org.opensearch.dataprepper.plugins.lambda.common.config.ThresholdOptions;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

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
    private Counter numberOfRequestsSuccessCounter;

    @Mock
    private Counter numberOfRecordsFailedCounter;
    @Mock
    private Counter numberOfRequestsFailedCounter;

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
        when(pluginMetrics.counter(eq(NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_SUCCESS))).thenReturn(
            numberOfRecordsSuccessCounter);
        when(pluginMetrics.counter(eq(NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_FAILED))).thenReturn(
            numberOfRecordsFailedCounter);
        when(pluginMetrics.counter(eq(NUMBER_OF_SUCCESSFUL_REQUESTS_TO_LAMBDA))).thenReturn(
                numberOfRecordsSuccessCounter);
        when(pluginMetrics.counter(eq(NUMBER_OF_FAILED_REQUESTS_TO_LAMBDA))).thenReturn(
                numberOfRecordsFailedCounter);
        when(pluginMetrics.timer(anyString())).thenReturn(lambdaLatencyMetric);
        when(pluginMetrics.gauge(anyString(), any(AtomicLong.class))).thenAnswer(
            invocation -> invocation.getArgument(1));

        ClientOptions clientOptions = new ClientOptions();
        when(lambdaProcessorConfig.getClientOptions()).thenReturn(clientOptions);
        when(lambdaProcessorConfig.getFunctionName()).thenReturn("test-function");
        when(lambdaProcessorConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        when(lambdaProcessorConfig.getInvocationType()).thenReturn(InvocationType.REQUEST_RESPONSE);
        BatchOptions batchOptions = mock(BatchOptions.class);
        ThresholdOptions thresholdOptions = mock(ThresholdOptions.class);
        when(lambdaProcessorConfig.getBatchOptions()).thenReturn(batchOptions);
        when(lambdaProcessorConfig.getWhenCondition()).thenReturn(null);
        when(lambdaProcessorConfig.getResponseEventsMatch()).thenReturn(false);

        // Mock AWS Authentication Options
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.US_EAST_1);
        when(awsAuthenticationOptions.getAwsStsRoleArn()).thenReturn("testRole");

        // Mock BatchOptions and ThresholdOptions
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
          responseCodecPluginSetting = new PluginSetting(responseCodecConfig.getPluginName(),
              responseCodecConfig.getPluginSettings());
        }

        // Mock PluginFactory to return the mocked responseCodec
        when(pluginFactory.loadPlugin(eq(InputCodec.class), any(PluginSetting.class))).thenReturn(
            responseCodec);

        // Instantiate the LambdaProcessor manually
        lambdaProcessor = new LambdaProcessor(pluginFactory, pluginMetrics, lambdaProcessorConfig,
            awsCredentialsSupplier, expressionEvaluator);

        // Mock InvokeResponse
        when(invokeResponse.payload()).thenReturn(SdkBytes.fromUtf8String("[{\"key\":\"value\"}]"));
        when(invokeResponse.statusCode()).thenReturn(200); // Success status code

        // Mock LambdaAsyncClient inside LambdaProcessor
        LambdaAsyncClient lambdaAsyncClientMock = mock(LambdaAsyncClient.class);
        setPrivateField(lambdaProcessor, "lambdaAsyncClient", lambdaAsyncClientMock);

        // Mock the invoke method to return a completed future
        CompletableFuture<InvokeResponse> invokeFuture = CompletableFuture.completedFuture(
            invokeResponse);
        when(lambdaAsyncClientMock.invoke(any(InvokeRequest.class))).thenReturn(invokeFuture);

        // Mock Response Codec parse method
        doNothing().when(responseCodec).parse(any(InputStream.class), any(Consumer.class));

    }

    private void populatePrivateFields() throws Exception {
        List<String> tagsOnMatchFailure = Collections.singletonList("failure_tag");
        // Use reflection to set the private fields
        setPrivateField(lambdaProcessor, "numberOfRecordsSuccessCounter",
            numberOfRecordsSuccessCounter);
        setPrivateField(lambdaProcessor, "numberOfRequestsSuccessCounter",
                numberOfRequestsSuccessCounter);
        setPrivateField(lambdaProcessor, "numberOfRecordsFailedCounter", numberOfRecordsFailedCounter);
        setPrivateField(lambdaProcessor, "numberOfRequestsFailedCounter", numberOfRequestsFailedCounter);
        setPrivateField(lambdaProcessor, "tagsOnMatchFailure", tagsOnMatchFailure);
        setPrivateField(lambdaProcessor, "lambdaCommonHandler", lambdaCommonHandler);
    }

    // Helper method to set private fields via reflection
    private void setPrivateField(Object targetObject, String fieldName, Object value)
      throws Exception {
        Field field = targetObject.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(targetObject, value);
    }

    @Test
    public void testProcessorDefaults() {
        // Create a new LambdaProcessorConfig with default values
        LambdaProcessorConfig defaultConfig = new LambdaProcessorConfig();

        // Test default values
        assertNull(defaultConfig.getFunctionName());
        assertNull(defaultConfig.getAwsAuthenticationOptions());
        assertNull(defaultConfig.getResponseCodecConfig());
        assertEquals(InvocationType.REQUEST_RESPONSE, defaultConfig.getInvocationType());
        assertFalse(defaultConfig.getResponseEventsMatch());
        assertNull(defaultConfig.getWhenCondition());
        assertTrue(defaultConfig.getTagsOnFailure().isEmpty());

        // Test ClientOptions defaults
        ClientOptions clientOptions = defaultConfig.getClientOptions();
        assertNotNull(clientOptions);
        assertEquals(ClientOptions.DEFAULT_CONNECTION_RETRIES, clientOptions.getMaxConnectionRetries());
        assertEquals(ClientOptions.DEFAULT_API_TIMEOUT, clientOptions.getApiCallTimeout());
        assertEquals(ClientOptions.DEFAULT_CONNECTION_TIMEOUT, clientOptions.getConnectionTimeout());
        assertEquals(ClientOptions.DEFAULT_MAXIMUM_CONCURRENCY, clientOptions.getMaxConcurrency());
        assertEquals(ClientOptions.DEFAULT_BASE_DELAY, clientOptions.getBaseDelay());
        assertEquals(ClientOptions.DEFAULT_MAX_BACKOFF, clientOptions.getMaxBackoff());

        // Test BatchOptions defaults
        BatchOptions batchOptions = defaultConfig.getBatchOptions();
        assertNotNull(batchOptions);
    }

    @Test
    public void testDoExecute_WithExceptionDuringProcessing() throws Exception {
        // Arrange
        Event event = mock(Event.class);
        Record<Event> record = new Record<>(event);
        List<Record<Event>> records = Collections.singletonList(record);

        // make batch options null to generate exception
        when(lambdaProcessorConfig.getBatchOptions()).thenReturn(null);
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
        lambdaProcessor = new LambdaProcessor(pluginFactory, pluginMetrics, lambdaProcessorConfig,
            awsCredentialsSupplier, expressionEvaluator);

        // Act
        Collection<Record<Event>> result = lambdaProcessor.doExecute(records);

        // Assert
        assertEquals(1, result.size(), "Result should contain one record as the condition is false.");
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
        CompletableFuture<InvokeResponse> invokeFuture = CompletableFuture.completedFuture(
            invokeResponse);
        when(lambdaAsyncClientMock.invoke(any(InvokeRequest.class))).thenReturn(invokeFuture);

        // Mock Buffer behavior
        when(bufferMock.getEventCount()).thenReturn(0).thenReturn(1).thenReturn(0);
        when(bufferMock.getRecords()).thenReturn(Collections.singletonList(record));

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
        List<Record<Event>> resultRecords = lambdaProcessor.convertLambdaResponseToEvent(bufferMock, invokeResponse);

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
        List<Record<Event>> resultRecords = lambdaProcessor.convertLambdaResponseToEvent(bufferMock, invokeResponse);
        // Assert
        // Verify that three records are added to the result
        assertEquals(3, resultRecords.size(), "ResultRecords should contain three records.");
    }

}
