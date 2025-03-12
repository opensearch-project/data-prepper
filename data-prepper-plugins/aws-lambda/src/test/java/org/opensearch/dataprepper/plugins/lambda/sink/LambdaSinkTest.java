package org.opensearch.dataprepper.plugins.lambda.sink;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.lambda.common.LambdaCommonHandler;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.InMemoryBuffer;
import org.opensearch.dataprepper.plugins.lambda.common.config.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.BatchOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.ClientOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.InvocationType;
import org.opensearch.dataprepper.plugins.lambda.common.config.ThresholdOptions;
import org.opensearch.dataprepper.plugins.lambda.sink.dlq.DlqPushHandler;
import org.opensearch.dataprepper.plugins.dlq.DlqProvider;
import org.opensearch.dataprepper.plugins.dlq.DlqWriter;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.lambda.utils.LambdaTestSetupUtil.getSampleRecord;

class LambdaSinkTest {

    @Mock
    private LambdaSinkConfig lambdaSinkConfig;
    @Mock
    private SinkContext sinkContext;
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private PluginFactory pluginFactory;
    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;
    @Mock
    private ExpressionEvaluator expressionEvaluator;

    // Counters
    @Mock
    private Counter numberOfRecordsSuccessCounter;
    @Mock
    private Counter numberOfRecordsFailedCounter;
    @Mock
    private Counter numberOfRequestsSuccessCounter;
    @Mock
    private Counter numberOfRequestsFailedCounter;

    // Timer and Summaries
    @Mock
    private Timer lambdaLatencyMetric;
    @Mock
    private DistributionSummary requestPayloadMetric;
    @Mock
    private DistributionSummary responsePayloadMetric;

    @Mock
    private DlqPushHandler dlqPushHandler;
    @Mock
    private DlqProvider dlqProvider;
    @Mock
    private DlqWriter dlqWriter;

    private PluginSetting pluginSetting;
    private LambdaSink lambdaSink;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);

        // Setup plugin metrics mocks
        when(pluginMetrics.counter(LambdaSink.NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_SUCCESS))
                .thenReturn(numberOfRecordsSuccessCounter);
        when(pluginMetrics.counter(LambdaSink.NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_FAILED))
                .thenReturn(numberOfRecordsFailedCounter);
        when(pluginMetrics.counter(LambdaSink.NUMBER_OF_SUCCESSFUL_REQUESTS_TO_LAMBDA))
                .thenReturn(numberOfRequestsSuccessCounter);
        when(pluginMetrics.counter(LambdaSink.NUMBER_OF_FAILED_REQUESTS_TO_LAMBDA))
                .thenReturn(numberOfRequestsFailedCounter);

        when(pluginMetrics.timer(LambdaSink.LAMBDA_LATENCY_METRIC)).thenReturn(lambdaLatencyMetric);
        when(pluginMetrics.summary(LambdaSink.REQUEST_PAYLOAD_SIZE)).thenReturn(requestPayloadMetric);
        when(pluginMetrics.summary(LambdaSink.RESPONSE_PAYLOAD_SIZE)).thenReturn(responsePayloadMetric);
        when(pluginMetrics.gauge(anyString(), any(AtomicLong.class))).thenReturn(new AtomicLong());

        // Mock the Batch/Threshold options
        final ThresholdOptions thresholdOptions = mock(ThresholdOptions.class);
        when(thresholdOptions.getEventCount()).thenReturn(2);  // flush after 2 events
        when(thresholdOptions.getMaximumSize()).thenReturn(ByteCount.parse("1mb"));
        when(thresholdOptions.getEventCollectTimeOut()).thenReturn(Duration.ofMinutes(5));

        final BatchOptions batchOptions = mock(BatchOptions.class);
        when(batchOptions.getKeyName()).thenReturn("testKey");
        when(batchOptions.getThresholdOptions()).thenReturn(thresholdOptions);

        // Setup LambdaSinkConfig
        when(lambdaSinkConfig.getFunctionName()).thenReturn("test-function");
        when(lambdaSinkConfig.getInvocationType()).thenReturn(InvocationType.EVENT);
        when(lambdaSinkConfig.getBatchOptions()).thenReturn(batchOptions);

        final AwsAuthenticationOptions awsAuthOptions = mock(AwsAuthenticationOptions.class);
        when(awsAuthOptions.getAwsRegion()).thenReturn(Region.US_EAST_1);
        when(lambdaSinkConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthOptions);

        final ClientOptions clientOptions = new ClientOptions();
        when(lambdaSinkConfig.getClientOptions()).thenReturn(clientOptions);

        // For DLQ
        when(lambdaSinkConfig.getDlqPluginSetting()).thenReturn(null); // default no DLQ

        // Create pluginSetting
        pluginSetting = new PluginSetting("aws_lambda", new HashMap<>());
        pluginSetting.setPipelineName("testPipeline");

        // Construct the LambdaSink
        lambdaSink = new LambdaSink(
                pluginSetting,
                lambdaSinkConfig,
                pluginFactory,
                sinkContext,
                awsCredentialsSupplier,
                expressionEvaluator
        );

        setPrivateField(lambdaSink, "numberOfRecordsSuccessCounter", numberOfRecordsSuccessCounter);
        setPrivateField(lambdaSink, "numberOfRecordsFailedCounter", numberOfRecordsFailedCounter);
        setPrivateField(lambdaSink, "numberOfRequestsSuccessCounter", numberOfRequestsSuccessCounter);
        setPrivateField(lambdaSink, "numberOfRequestsFailedCounter", numberOfRequestsFailedCounter);
        setPrivateField(lambdaSink, "lambdaLatencyMetric", lambdaLatencyMetric);
        setPrivateField(lambdaSink, "requestPayloadMetric", requestPayloadMetric);
        setPrivateField(lambdaSink, "responsePayloadMetric", responsePayloadMetric);

        // Initialize the sink
        lambdaSink.doInitialize();
    }

    @Test
    void testNoFlushIfThresholdNotReached() {
        // threshold=2, only pass 1 record => no flush
        final List<Record<Event>> records = getRecords(1);

        // We expect no call to invokeLambdaAndGetFutureMap(...) since threshold not hit
        try (MockedStatic<LambdaCommonHandler> mockedHandler = mockStatic(LambdaCommonHandler.class)) {

            lambdaSink.doOutput(records);

            // Because threshold=2 and we only provided 1 event, no flush => 0 calls
            mockedHandler.verify(
                    () -> LambdaCommonHandler.invokeLambdaAndGetFutureMap(any(), any(), anyList()),
                    never()
            );
        }

        // Also no success or fail increments
        verify(numberOfRecordsSuccessCounter, never()).increment(anyDouble());
        verify(numberOfRecordsFailedCounter, never()).increment(anyDouble());
        verify(numberOfRequestsSuccessCounter, never()).increment();
        verify(numberOfRequestsFailedCounter, never()).increment();
    }

    @Test
    void testFlushWhenThresholdReached() {
        // threshold=2, pass 2 => flush
        final List<Record<Event>> records = getRecords(2);

        // Mock static calls
        try (MockedStatic<LambdaCommonHandler> mockedHandler = mockStatic(LambdaCommonHandler.class)) {
            // We'll let isSuccess(...) call real method so it checks statusCode
            mockedHandler.when(() -> LambdaCommonHandler.isSuccess(any()))
                    .thenCallRealMethod();

            final InvokeResponse mockResponse = mock(InvokeResponse.class);
            when(mockResponse.statusCode()).thenReturn(200);  // success
            when(mockResponse.payload()).thenReturn(SdkBytes.fromUtf8String("{\"msg\":\"OK\"}"));

            // Future that returns mockResponse
            CompletableFuture<InvokeResponse> completedFuture = mock(CompletableFuture.class);
            when(completedFuture.join()).thenReturn(mockResponse);

            // One buffer => future mapping
            final Buffer mockBuffer = mock(Buffer.class);
            when(mockBuffer.getRecords()).thenReturn(records);
            when(mockBuffer.getEventCount()).thenReturn(2);

            final Map<Buffer, CompletableFuture<InvokeResponse>> resultMap =
                    Map.of(mockBuffer, completedFuture);

            // Now, because flushBuffers(...) calls invokeLambdaAndGetFutureMap(...),
            // we mock that:
            mockedHandler.when(() ->
                    LambdaCommonHandler.invokeLambdaAndGetFutureMap(any(), any(), anyList())
            ).thenReturn(resultMap);

            // ACT
            lambdaSink.doOutput(records);

            // Since threshold=2 => flush => exactly 1 call to invokeLambdaAndGetFutureMap
            mockedHandler.verify(() ->
                            LambdaCommonHandler.invokeLambdaAndGetFutureMap(any(), any(), anyList()),
                    times(1)
            );

            // We expect success
            verify(numberOfRecordsSuccessCounter).increment(2.0); // 2 events
            verify(numberOfRequestsSuccessCounter).increment();
            verify(numberOfRecordsFailedCounter, never()).increment(anyDouble());
            verify(numberOfRequestsFailedCounter, never()).increment();
        }
    }

    private static List<Record<Event>> getRecords(int numberOfRecords) {
        final Map<String, Object> data = new HashMap<>();
        data.put("payload", "a");
        final Event event = JacksonEvent.builder()
                .withData(data)
                .withEventType("test")
                .build();
        Record<Event> record = new Record<>(event);
        List<Record<Event>> records = new ArrayList();
        for(int i=0;i<numberOfRecords;i++){
            records.add(record);
        }
        return records;
    }

    @Test
    void testShutdownFlushesPartialIfAny() {
        // threshold=2, pass only 1 => partial
        final List<Record<Event>> records = getRecords(1);

        try (MockedStatic<LambdaCommonHandler> mockedHandler = mockStatic(LambdaCommonHandler.class)) {
            mockedHandler.when(() -> LambdaCommonHandler.isSuccess(any()))
                    .thenCallRealMethod();
            // partial => no flush
            lambdaSink.doOutput(records);

            mockedHandler.verify(() ->
                            LambdaCommonHandler.invokeLambdaAndGetFutureMap(any(), any(), anyList()),
                    never()
            );

            // Now shutdown => leftover partial => flush once
            final InvokeResponse mockResponse = mock(InvokeResponse.class);
            when(mockResponse.statusCode()).thenReturn(200);
            when(mockResponse.payload()).thenReturn(SdkBytes.fromUtf8String("{\"msg\":\"OK\"}"));

            CompletableFuture<InvokeResponse> completedFuture =
                    CompletableFuture.completedFuture(mockResponse);

            final Buffer mockBuffer = mock(Buffer.class);
            when(mockBuffer.getRecords()).thenReturn(records);
            when(mockBuffer.getEventCount()).thenReturn(1);

            mockedHandler.when(() ->
                    LambdaCommonHandler.invokeLambdaAndGetFutureMap(any(), any(), anyList())
            ).thenReturn(Map.of(mockBuffer, completedFuture));

            // Trigger shutdown
            lambdaSink.shutdown();

            // We now expect exactly 1 call
            mockedHandler.verify(() ->
                            LambdaCommonHandler.invokeLambdaAndGetFutureMap(any(), any(), anyList()),
                    times(1)
            );

            // success counters
            verify(numberOfRecordsSuccessCounter).increment(1.0);
            verify(numberOfRequestsSuccessCounter).increment();
        }
    }

    @Test
    void testFailureDuringInvokeLambdaAndGetFutureMap() {
        // pass 2 => threshold => flush => but an exception is thrown
        final List<Record<Event>> records = getRecords(2);

        try (MockedStatic<LambdaCommonHandler> mockedHandler = mockStatic(LambdaCommonHandler.class)) {
            // cause the method to throw an exception
            mockedHandler.when(() ->
                    LambdaCommonHandler.invokeLambdaAndGetFutureMap(any(), any(), anyList())
            ).thenThrow(new RuntimeException("Test flush error"));

            lambdaSink.doOutput(records);

            // We expect fail counters
            verify(numberOfRecordsFailedCounter).increment(2.0);
            verify(numberOfRequestsFailedCounter).increment();
            verify(numberOfRecordsSuccessCounter, never()).increment(anyDouble());
        }
    }

    @Test
    void testFailureInFutureJoin() {
        // pass 2 => threshold => flush => future join fails
        final List<Record<Event>> records = getRecords(2);

        try (MockedStatic<LambdaCommonHandler> mockedHandler = mockStatic(LambdaCommonHandler.class)) {
            final CompletableFuture<InvokeResponse> failingFuture = new CompletableFuture<>();
            failingFuture.completeExceptionally(new RuntimeException("InvokeResponse error"));

            final Buffer bufferMock = mock(Buffer.class);
            when(bufferMock.getRecords()).thenReturn(records);
            when(bufferMock.getEventCount()).thenReturn(2);

            final Map<Buffer, CompletableFuture<InvokeResponse>> mapResult =
                    Map.of(bufferMock, failingFuture);

            mockedHandler.when(() ->
                    LambdaCommonHandler.invokeLambdaAndGetFutureMap(any(), any(), anyList())
            ).thenReturn(mapResult);

            lambdaSink.doOutput(records);

            // Because future threw an error, we expect failure counters
            verify(numberOfRecordsFailedCounter).increment(2.0);
            verify(numberOfRequestsFailedCounter).increment();
            verify(numberOfRecordsSuccessCounter, never()).increment(anyDouble());
        }
    }

    @Test
    public void testHandleFailure_WithDlq() throws Exception {
        Throwable throwable = new RuntimeException("Test Exception");
        Buffer buffer = new InMemoryBuffer(UUID.randomUUID().toString());
        buffer.addRecord(getSampleRecord());
        setPrivateField(lambdaSink, "dlqPushHandler", dlqPushHandler);
        setPrivateField(lambdaSink, "numberOfRecordsFailedCounter", numberOfRecordsFailedCounter);
        lambdaSink.handleFailure(buffer.getRecords(), throwable, 0);
        verify(numberOfRecordsFailedCounter, times(1)).increment(1.0);
        verify(dlqPushHandler, times(1)).perform(anyList());
    }

    @Test
    public void testHandleFailure_WithoutDlq() throws Exception {
        Throwable throwable = new RuntimeException("Test Exception");
        Buffer buffer = new InMemoryBuffer(UUID.randomUUID().toString());
        buffer.addRecord(getSampleRecord());
        when(lambdaSinkConfig.getDlqPluginSetting()).thenReturn(null);
        setPrivateField(lambdaSink, "numberOfRecordsFailedCounter", numberOfRecordsFailedCounter);
        lambdaSink.handleFailure(buffer.getRecords(), throwable, 0);
        verify(numberOfRecordsFailedCounter, times(1)).increment(1);
        verify(dlqPushHandler, never()).perform(anyList());
    }

    @Test
    void testFlushDueToTimeoutWhenSecondCallHasNoRecords() throws Exception {
        // Set the maxCollectTime to a very short duration so that the buffer will be considered timed-out.
        setPrivateField(lambdaSink, "maxCollectTime", Duration.ofMillis(1));

        // Create a spy on lambdaSink to intercept flushBuffers() calls.
        LambdaSink spySink = spy(lambdaSink);

        // Use an AtomicBoolean flag to record whether flushBuffers() is called.
        AtomicBoolean flushCalled = new AtomicBoolean(false);
        doAnswer(invocation -> {
            flushCalled.set(true);
            return null;
        }).when(spySink).flushBuffers(anyList());

        // First call: add one record so that the buffer is non-empty.
        List<Record<Event>> records = getRecords(1);
        spySink.doOutput(records);

        // Wait briefly to allow the buffer's duration to exceed maxCollectTime.
        Thread.sleep(10);

        // Second call: pass an empty collection; this should trigger the timeout flush.
        spySink.doOutput(Collections.emptyList());

        // Verify that flushBuffers() was called due to the timeout.
        assertTrue(flushCalled.get(), "Expected flushBuffers() to be called on the second call due to timeout.");
    }

    @Test
    void testConcurrentDoOutputWithMultipleThreads() throws Exception {
        // Set maxCollectTime to a very short duration to force the timeout flush.
        setPrivateField(lambdaSink, "maxCollectTime", Duration.ofMillis(1));

        // Create a spy of the sink so we can intercept flushBuffers() calls.
        LambdaSink spySink = spy(lambdaSink);

        // Use an AtomicInteger to count how many times flushBuffers() is called.
        AtomicInteger flushCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            flushCount.incrementAndGet();
            return null;
        }).when(spySink).flushBuffers(anyList());

        // Create a thread pool with multiple threads.
        int threadCount = 20;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        int iterations = 1000;
        List<Future<?>> futures = new ArrayList<>();

        // Add one record to the sink so that the buffer is non-empty.
        List<Record<Event>> records = getRecords(1);
        // Each task calls doOutput() with one record.
        for (int i = 0; i < iterations; i++) {
            futures.add(executor.submit(() -> {
                spySink.doOutput(records);
            }));
        }

        // Wait for all tasks to complete.
        for (Future<?> future : futures) {
            future.get();
        }

        // Additionally, call doOutput() with an empty collection to trigger any pending timeout flush.
        spySink.doOutput(Collections.emptyList());

        executor.shutdown();

        // Assert that flushBuffers() was called at least once (or more) under concurrent load.
        // With a short timeout, even if each call adds a record, eventually the partial buffer becomes "old"
        // and flushes. If the locking is working correctly, flushCount should be > 0.
        assertTrue(flushCount.get() > 0, "Expected at least one flush due to timeout under concurrent calls, but flushCount is " + flushCount.get());
    }



    // Utility to set private fields
    private static void setPrivateField(Object target, String fieldName, Object value) {
        try {
            Field f = target.getClass().getDeclaredField(fieldName);
            f.setAccessible(true);
            f.set(target, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
