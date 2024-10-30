package org.opensearch.dataprepper.plugins.lambda.processor;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import org.mockito.Mock;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.MockitoAnnotations;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.DefaultEventHandle;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.codec.json.JsonInputCodec;
import org.opensearch.dataprepper.plugins.lambda.common.LambdaCommonHandler;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.InMemoryBuffer;
import org.opensearch.dataprepper.plugins.lambda.common.config.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.BatchOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.InvocationType;
import org.opensearch.dataprepper.plugins.lambda.common.config.ThresholdOptions;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
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

public class LambdaProcessorTest {

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private LambdaProcessorConfig lambdaProcessorConfig;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    AwsAuthenticationOptions awsAuthenticationOptions;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    @Mock
    private LambdaCommonHandler lambdaCommonHandler;

    @Mock
    private OutputCodec requestCodec;

    @Mock
    private JsonInputCodec responseCodec;

    @Mock
    private InMemoryBuffer currentBufferPerBatch;

    @Mock
    private Counter numberOfRecordsSuccessCounter;

    @Mock
    private Counter numberOfRecordsFailedCounter;

    @Mock
    private InvokeResponse invokeResponse;

    @Mock
    private Event event;

    @Mock
    private EventMetadata eventMetadata;

    private LambdaProcessor lambdaProcessor;

    @BeforeEach
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        // Mock PluginMetrics counters and timers
        when(pluginMetrics.counter(anyString())).thenReturn(numberOfRecordsSuccessCounter);
        when(pluginMetrics.timer(anyString())).thenReturn(mock(Timer.class));
        when(pluginMetrics.gauge(anyString(), any(AtomicLong.class))).thenReturn(new AtomicLong());

        // Mock lambdaProcessorConfig
        when(lambdaProcessorConfig.getFunctionName()).thenReturn("test-function");
        when(lambdaProcessorConfig.getWhenCondition()).thenReturn(null);
        when(lambdaProcessorConfig.getInvocationType()).thenReturn(InvocationType.REQUEST_RESPONSE);
        when(lambdaProcessorConfig.getResponseEventsMatch()).thenReturn(Boolean.FALSE);
        when(lambdaProcessorConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
        when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.US_EAST_1);
        when(awsAuthenticationOptions.getAwsStsRoleArn()).thenReturn("testRole");
        when(awsAuthenticationOptions.getAwsStsHeaderOverrides()).thenReturn(null);

        // Mock BatchOptions and ThresholdOptions
        BatchOptions batchOptions = mock(BatchOptions.class);
        ThresholdOptions thresholdOptions = mock(ThresholdOptions.class);

        // Set up the mocks to return default values
        when(lambdaProcessorConfig.getBatchOptions()).thenReturn(batchOptions);
        when(lambdaProcessorConfig.getConnectionTimeout()).thenReturn(Duration.ofSeconds(5));
        when(batchOptions.getThresholdOptions()).thenReturn(thresholdOptions);
        when(thresholdOptions.getEventCount()).thenReturn(100); // Set a default event count
        when(thresholdOptions.getMaximumSize()).thenReturn(ByteCount.parse("6mb"));
        when(thresholdOptions.getEventCollectTimeOut()).thenReturn(Duration.ofSeconds(30));

        // Mock lambdaCommonHandler.createBuffer() to return currentBufferPerBatch
        when(lambdaCommonHandler.createBuffer(any())).thenReturn(currentBufferPerBatch);

        // Mock currentBufferPerBatch.reset()
        doNothing().when(currentBufferPerBatch).reset();
    }

    private void setupTestObject() {
        // Create the LambdaProcessor instance
        lambdaProcessor = new LambdaProcessor(pluginFactory, pluginMetrics, lambdaProcessorConfig,
                awsCredentialsSupplier, expressionEvaluator);
    }

    private void populatePrivateFields() throws Exception {
        List<String> tagsOnMatchFailure = Collections.singletonList("failure_tag");
        // Use reflection to set the private fields
        setPrivateField(lambdaProcessor, "lambdaCommonHandler", lambdaCommonHandler);
        setPrivateField(lambdaProcessor, "requestCodec", requestCodec);
        setPrivateField(lambdaProcessor, "responseCodec", responseCodec);
        setPrivateField(lambdaProcessor, "currentBufferPerBatch", currentBufferPerBatch);
        setPrivateField(lambdaProcessor, "futureList", new ArrayList<>());
        setPrivateField(lambdaProcessor, "numberOfRecordsSuccessCounter", numberOfRecordsSuccessCounter);
        setPrivateField(lambdaProcessor, "numberOfRecordsFailedCounter", numberOfRecordsFailedCounter);
        setPrivateField(lambdaProcessor, "tagsOnMatchFailure", tagsOnMatchFailure);
    }


    // Helper method to set private fields via reflection
    private void setPrivateField(Object targetObject, String fieldName, Object value) throws Exception {
        Field field = targetObject.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(targetObject, value);
    }

    @Test
    public void testDoExecute_WithEmptyRecords() throws Exception {
        // Arrange
        setupTestObject();
        populatePrivateFields();
        Collection<Record<Event>> records = Collections.emptyList();

        // Act
        Collection<Record<Event>> result = lambdaProcessor.doExecute(records);

        // Assert
        assert result.isEmpty();
    }

    @Test
    public void testDoExecute_WithRecords_WhenConditionFalse() throws Exception {
        // Arrange
        Event event = mock(Event.class);
        Record<Event> record = new Record<>(event);
        Collection<Record<Event>> records = Collections.singletonList(record);

        when(expressionEvaluator.evaluateConditional(anyString(), eq(event))).thenReturn(false);
        when(lambdaProcessorConfig.getWhenCondition()).thenReturn("some_condition");
        setupTestObject();
        populatePrivateFields();

        // Act
        Collection<Record<Event>> result = lambdaProcessor.doExecute(records);

        // Assert
        assert result.size() == 1;
        assert result.iterator().next() == record;
    }

    @Test
    public void testDoExecute_WithRecords_SuccessfulProcessing() throws Exception {
        // Arrange
        when(lambdaProcessorConfig.getResponseEventsMatch()).thenReturn(Boolean.TRUE);
        Event event = mock(Event.class);
        Record<Event> record = new Record<>(event);
        Collection<Record<Event>> records = Collections.singletonList(record);

        // Mock EventMetadata
        EventMetadata responseEventMetadata = mock(EventMetadata.class);
        when(event.getMetadata()).thenReturn(responseEventMetadata);

        // Mock currentBufferPerBatch behavior
        when(currentBufferPerBatch.getEventCount()).thenReturn(0).thenReturn(1);
        doNothing().when(requestCodec).start(any(), any(), any());
        doNothing().when(requestCodec).writeEvent(any(), any());
        when(currentBufferPerBatch.getRecords()).thenReturn(Collections.singletonList(record));
        doNothing().when(currentBufferPerBatch).reset();

        // Mocking Lambda invocation
        InvokeResponse invokeResponse = mock(InvokeResponse.class);
        CompletableFuture<InvokeResponse> invokeFuture = CompletableFuture.completedFuture(invokeResponse);
        when(currentBufferPerBatch.flushToLambda(any())).thenReturn(invokeFuture);
        doNothing().when(requestCodec).complete(any());

        // Set up invokeResponse payload and status code
        String payloadString = "[{\"key\":\"value\"}]";
        SdkBytes sdkBytes = SdkBytes.fromByteArray(payloadString.getBytes());
        when(invokeResponse.payload()).thenReturn(sdkBytes);
        when(invokeResponse.statusCode()).thenReturn(200); // Ensure success status code
        when(lambdaCommonHandler.checkStatusCode(any())).thenReturn(true);
        // Mock responseCodec parsing
        doAnswer(invocation -> {
            InputStream inputStream = (InputStream) invocation.getArgument(0);
            Consumer<Record<Event>> consumer = (Consumer<Record<Event>>) invocation.getArgument(1);
            Event responseEvent = JacksonLog.builder().withData(Collections.singletonMap("key", "value")).build();
            consumer.accept(new Record<>(responseEvent));
            return null;
        }).when(responseCodec).parse(any(InputStream.class), any(Consumer.class));

        // Mock lambdaCommonHandler.createBuffer() to return currentBufferPerBatch
        when(lambdaCommonHandler.createBuffer(any())).thenReturn(currentBufferPerBatch);
        setupTestObject();
        populatePrivateFields();

        // Act
        Collection<Record<Event>> result = lambdaProcessor.doExecute(records);

        // Wait for futures to complete
        lambdaProcessor.lambdaCommonHandler.waitForFutures(lambdaProcessor.futureList);

        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        Record<Event> resultRecord = result.iterator().next();
        Event resultEvent = resultRecord.getData();

        // Verify that the original event was updated
        verify(event, times(1)).clear();

        // Verify that currentBufferPerBatch.reset() was called
        verify(currentBufferPerBatch, times(1)).reset();
    }

    @Test
    public void testHandleFailure() throws Exception {
        // Arrange
        setupTestObject();
        populatePrivateFields();
        Throwable throwable = new RuntimeException("Test Exception");
        Buffer flushedBuffer = mock(InMemoryBuffer.class);
        List<Record<Event>> originalRecords = new ArrayList<>();
        Event event = JacksonEvent.builder().withEventType("event").withData("{\"status\":true}").build();
        Record<Event> record = new Record<>(event);
        originalRecords.add(record);
        when(flushedBuffer.getEventCount()).thenReturn(1);
        when(flushedBuffer.getRecords()).thenReturn(originalRecords);

        // Act
        lambdaProcessor.handleFailure(throwable, flushedBuffer, new ArrayList<>());

        // Assert
        verify(numberOfRecordsFailedCounter, times(1)).increment(1.0);
    }

    @Test
    public void testConvertLambdaResponseToEvent_WithEqualEventCounts_SuccessfulProcessing() throws Exception {
        // Arrange
        when(lambdaProcessorConfig.getResponseEventsMatch()).thenReturn(Boolean.TRUE);
        setupTestObject();
        populatePrivateFields();
        List<Record<Event>> resultRecords = new ArrayList<>();

        // Mock LambdaResponse with a valid payload
        String payloadString = "[{\"key\":\"value1\"}, {\"key\":\"value2\"}]";
        SdkBytes sdkBytes = SdkBytes.fromByteArray(payloadString.getBytes());
        when(invokeResponse.payload()).thenReturn(sdkBytes);
        when(invokeResponse.statusCode()).thenReturn(200); // Success status code

        // Mock the responseCodec.parse to add two events
        doAnswer(invocation -> {
            InputStream inputStream = (InputStream) invocation.getArgument(0);
            @SuppressWarnings("unchecked")
            Consumer<Record<Event>> consumer = (Consumer<Record<Event>>) invocation.getArgument(1);
            Event parsedEvent1 = mock(Event.class);
            EventMetadata parsedEventMetadata1 = mock(EventMetadata.class);
            when(parsedEvent1.getMetadata()).thenReturn(parsedEventMetadata1);

            Event parsedEvent2 = mock(Event.class);
            EventMetadata parsedEventMetadata2 = mock(EventMetadata.class);
            when(parsedEvent2.getMetadata()).thenReturn(parsedEventMetadata2);

            consumer.accept(new Record<>(parsedEvent1));
            consumer.accept(new Record<>(parsedEvent2));
            return null;
        }).when(responseCodec).parse(any(InputStream.class), any(Consumer.class));

        // Mock buffer with two original events
        Event originalEvent1 = mock(Event.class);
        EventMetadata originalEventMetadata1 = mock(EventMetadata.class);
        when(originalEvent1.getMetadata()).thenReturn(originalEventMetadata1);

        Event originalEvent2 = mock(Event.class);
        EventMetadata originalEventMetadata2 = mock(EventMetadata.class);
        when(originalEvent2.getMetadata()).thenReturn(originalEventMetadata2);

        List<Record<Event>> originalRecords = Arrays.asList(
                new Record<>(originalEvent1),
                new Record<>(originalEvent2)
        );

        Buffer flushedBuffer = mock(Buffer.class);
        when(flushedBuffer.getEventCount()).thenReturn(2);
        when(flushedBuffer.getRecords()).thenReturn(originalRecords);

        // Act
        lambdaProcessor.convertLambdaResponseToEvent(resultRecords, invokeResponse, flushedBuffer);

        // Assert
        assertNotNull(resultRecords);
        assertEquals(2, resultRecords.size(), "ResultRecords should contain two records");

        //Verify
        verify(originalEvent1, times(1)).clear();
        verify(originalEvent2, times(1)).clear();

    }

    @Test
    public void testConvertLambdaResponseToEvent_WithUnequalEventCounts_SuccessfulProcessing() throws Exception {
        // Arrange
        when(lambdaProcessorConfig.getResponseEventsMatch()).thenReturn(Boolean.FALSE);
        setupTestObject();
        populatePrivateFields();
        List<Record<Event>> resultRecords = new ArrayList<>();

        // Mock LambdaResponse with a valid payload containing three events
        String payloadString = "[{\"key\":\"value1\"}, {\"key\":\"value2\"}, {\"key\":\"value3\"}]";
        SdkBytes sdkBytes = SdkBytes.fromByteArray(payloadString.getBytes());
        when(invokeResponse.payload()).thenReturn(sdkBytes);
        when(invokeResponse.statusCode()).thenReturn(200); // Success status code
        when(lambdaCommonHandler.checkStatusCode(any())).thenReturn(true);
        // Mock the responseCodec.parse to add three events
        doAnswer(invocation -> {
            InputStream inputStream = (InputStream) invocation.getArgument(0);
            Consumer<Record<Event>> consumer = (Consumer<Record<Event>>) invocation.getArgument(1);
            Event parsedEvent1 = mock(Event.class);
            EventMetadata parsedEventMetadata1 = mock(EventMetadata.class);
            when(parsedEvent1.getMetadata()).thenReturn(parsedEventMetadata1);

            Event parsedEvent2 = mock(Event.class);
            EventMetadata parsedEventMetadata2 = mock(EventMetadata.class);
            when(parsedEvent2.getMetadata()).thenReturn(parsedEventMetadata2);

            Event parsedEvent3 = mock(Event.class);
            EventMetadata parsedEventMetadata3 = mock(EventMetadata.class);
            when(parsedEvent3.getMetadata()).thenReturn(parsedEventMetadata3);

            consumer.accept(new Record<>(parsedEvent1));
            consumer.accept(new Record<>(parsedEvent2));
            consumer.accept(new Record<>(parsedEvent3));
            return null;
        }).when(responseCodec).parse(any(InputStream.class), any(Consumer.class));

        // Mock buffer with two original events
        Event originalEvent1 = mock(Event.class);
        EventMetadata originalEventMetadata1 = mock(EventMetadata.class);
        when(originalEvent1.getMetadata()).thenReturn(originalEventMetadata1);

        Event originalEvent2 = mock(Event.class);
        EventMetadata originalEventMetadata2 = mock(EventMetadata.class);
        when(originalEvent2.getMetadata()).thenReturn(originalEventMetadata2);

        List<Record<Event>> originalRecords = Arrays.asList(
                new Record<>(originalEvent1),
                new Record<>(originalEvent2)
        );

        Buffer flushedBuffer = mock(Buffer.class);
        when(flushedBuffer.getEventCount()).thenReturn(2);
        when(flushedBuffer.getRecords()).thenReturn(originalRecords);

        // Mock acknowledgement set
        DefaultEventHandle eventHandle = mock(DefaultEventHandle.class);
        AcknowledgementSet acknowledgementSet = mock(AcknowledgementSet.class);
        when(originalEvent1.getEventHandle()).thenReturn(eventHandle);
        when(eventHandle.getAcknowledgementSet()).thenReturn(acknowledgementSet);

        // Act
        lambdaProcessor.convertLambdaResponseToEvent(resultRecords, invokeResponse, flushedBuffer);

        // Assert
        assertNotNull(resultRecords);
        assertEquals(3, resultRecords.size(), "ResultRecords should contain three records");

        // Verify that original events were not cleared or updated
        verify(originalEvent1, times(0)).clear();
        verify(originalEvent2, times(0)).clear();
        verify(acknowledgementSet, times(3)).add(any(Event.class));
    }

    @Test
    public void testConvertLambdaResponseToEvent_WithUnequalEventCounts_FailOn_STRICT_Mode() throws Exception {
        // Arrange
        List<Record<Event>> resultRecords = new ArrayList<>();

        // Mock LambdaResponse with a valid payload containing three events
        String payloadString = "[{\"key\":\"value1\"}, {\"key\":\"value2\"}, {\"key\":\"value3\"}]";
        SdkBytes sdkBytes = SdkBytes.fromByteArray(payloadString.getBytes());
        when(invokeResponse.payload()).thenReturn(sdkBytes);
        when(invokeResponse.statusCode()).thenReturn(200); // Success status code
        when(lambdaCommonHandler.checkStatusCode(any())).thenReturn(true);
        when(lambdaProcessorConfig.getResponseEventsMatch()).thenReturn(Boolean.TRUE);

        // Mock the responseCodec.parse to add three events
        doAnswer(invocation -> {
            InputStream inputStream = (InputStream) invocation.getArgument(0);
            Consumer<Record<Event>> consumer = (Consumer<Record<Event>>) invocation.getArgument(1);
            Event parsedEvent1 = mock(Event.class);
            EventMetadata parsedEventMetadata1 = mock(EventMetadata.class);
            when(parsedEvent1.getMetadata()).thenReturn(parsedEventMetadata1);

            Event parsedEvent2 = mock(Event.class);
            EventMetadata parsedEventMetadata2 = mock(EventMetadata.class);
            when(parsedEvent2.getMetadata()).thenReturn(parsedEventMetadata2);

            Event parsedEvent3 = mock(Event.class);
            EventMetadata parsedEventMetadata3 = mock(EventMetadata.class);
            when(parsedEvent3.getMetadata()).thenReturn(parsedEventMetadata3);

            consumer.accept(new Record<>(parsedEvent1));
            consumer.accept(new Record<>(parsedEvent2));
            consumer.accept(new Record<>(parsedEvent3));
            return null;
        }).when(responseCodec).parse(any(InputStream.class), any(Consumer.class));

        // Mock buffer with two original events
        Event originalEvent1 = mock(Event.class);
        EventMetadata originalEventMetadata1 = mock(EventMetadata.class);
        when(originalEvent1.getMetadata()).thenReturn(originalEventMetadata1);

        Event originalEvent2 = mock(Event.class);
        EventMetadata originalEventMetadata2 = mock(EventMetadata.class);
        when(originalEvent2.getMetadata()).thenReturn(originalEventMetadata2);

        List<Record<Event>> originalRecords = Arrays.asList(
                new Record<>(originalEvent1),
                new Record<>(originalEvent2)
        );

        Buffer flushedBuffer = mock(Buffer.class);
        when(flushedBuffer.getEventCount()).thenReturn(2);
        when(flushedBuffer.getRecords()).thenReturn(originalRecords);

        // Mock acknowledgement set
        DefaultEventHandle eventHandle = mock(DefaultEventHandle.class);
        AcknowledgementSet acknowledgementSet = mock(AcknowledgementSet.class);
        when(originalEvent1.getEventHandle()).thenReturn(eventHandle);
        when(eventHandle.getAcknowledgementSet()).thenReturn(acknowledgementSet);
        setupTestObject();
        populatePrivateFields();

        // Act
        lambdaProcessor.convertLambdaResponseToEvent(resultRecords, invokeResponse, flushedBuffer);

        verify(numberOfRecordsFailedCounter, times(1)).increment(2);

    }

}
