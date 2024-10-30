package org.opensearch.dataprepper.plugins.lambda.sink;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.MockitoAnnotations;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodec;
import org.opensearch.dataprepper.plugins.lambda.common.LambdaCommonHandler;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.BufferFactory;
import org.opensearch.dataprepper.plugins.lambda.common.config.BatchOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.InvocationType;
import org.opensearch.dataprepper.plugins.lambda.common.config.ThresholdOptions;
import org.opensearch.dataprepper.plugins.lambda.sink.dlq.DlqPushHandler;
import org.opensearch.dataprepper.plugins.lambda.sink.dlq.LambdaSinkFailedDlqData;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

public class LambdaSinkServiceTest {

    @Mock
    private LambdaAsyncClient lambdaAsyncClient;

    @Mock
    private LambdaSinkConfig lambdaSinkConfig;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private PluginSetting pluginSetting;

    @Mock
    private OutputCodecContext codecContext;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private DlqPushHandler dlqPushHandler;

    @Mock
    private BufferFactory bufferFactory;

    @Mock
    private ExpressionEvaluator expressionEvaluator;

    @Mock
    private Counter numberOfRecordsSuccessCounter;

    @Mock
    private Counter numberOfRecordsFailedCounter;

    @Mock
    private Timer lambdaLatencyMetric;

    @Mock
    private OutputCodec requestCodec;

    @Mock
    private Buffer currentBufferPerBatch;

    @Mock
    private LambdaCommonHandler lambdaCommonHandler;

    @Mock
    private Event event;

    @Mock
    private EventHandle eventHandle;

    @Mock
    private EventMetadata eventMetadata;

    @Mock
    private InvokeResponse invokeResponse;

    private LambdaSinkService lambdaSinkService;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        // Mock PluginMetrics counters and timers
        when(pluginMetrics.counter("lambdaSinkObjectsEventsSucceeded")).thenReturn(numberOfRecordsSuccessCounter);
        when(pluginMetrics.counter("lambdaSinkObjectsEventsFailed")).thenReturn(numberOfRecordsFailedCounter);
        when(pluginMetrics.timer(anyString())).thenReturn(lambdaLatencyMetric);
        when(pluginMetrics.gauge(anyString(), any(AtomicLong.class))).thenReturn(new AtomicLong());

        // Mock lambdaSinkConfig
        when(lambdaSinkConfig.getFunctionName()).thenReturn("test-function");
        when(lambdaSinkConfig.getWhenCondition()).thenReturn(null);
        when(lambdaSinkConfig.getInvocationType()).thenReturn(InvocationType.EVENT);

        // Mock BatchOptions and ThresholdOptions
        BatchOptions batchOptions = mock(BatchOptions.class);
        ThresholdOptions thresholdOptions = mock(ThresholdOptions.class);
        when(lambdaSinkConfig.getBatchOptions()).thenReturn(batchOptions);
        when(batchOptions.getThresholdOptions()).thenReturn(thresholdOptions);
        when(thresholdOptions.getEventCount()).thenReturn(10);
        when(thresholdOptions.getMaximumSize()).thenReturn(ByteCount.parse("1mb"));
        when(thresholdOptions.getEventCollectTimeOut()).thenReturn(Duration.ofSeconds(1));

        // Mock JsonOutputCodec
        requestCodec = mock(JsonOutputCodec.class);
        when(pluginFactory.loadPlugin(eq(OutputCodec.class), any(PluginSetting.class))).thenReturn(requestCodec);

        // Initialize bufferFactory and buffer
        bufferFactory = mock(BufferFactory.class);
        currentBufferPerBatch = mock(Buffer.class);
        when(currentBufferPerBatch.getEventCount()).thenReturn(0);

        // Mock LambdaCommonHandler
        lambdaCommonHandler = mock(LambdaCommonHandler.class);
        when(lambdaCommonHandler.createBuffer(any())).thenReturn(currentBufferPerBatch);
        doNothing().when(currentBufferPerBatch).reset();

        lambdaSinkService = new LambdaSinkService(
                lambdaAsyncClient,
                lambdaSinkConfig,
                pluginMetrics,
                pluginFactory,
                pluginSetting,
                codecContext,
                awsCredentialsSupplier,
                dlqPushHandler,
                bufferFactory,
                expressionEvaluator
        );

        // Set private fields
        setPrivateField(lambdaSinkService, "lambdaCommonHandler", lambdaCommonHandler);
        setPrivateField(lambdaSinkService, "requestCodec", requestCodec);
        setPrivateField(lambdaSinkService, "currentBufferPerBatch", currentBufferPerBatch);
    }

    // Helper method to set private fields via reflection
    private void setPrivateField(Object targetObject, String fieldName, Object value) {
        try {
            Field field = targetObject.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(targetObject, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testOutput_SuccessfulProcessing() throws Exception {
        Event event = mock(Event.class);
        Record<Event> record = new Record<>(event);
        Collection<Record<Event>> records = Collections.singletonList(record);

        when(expressionEvaluator.evaluateConditional(anyString(), eq(event))).thenReturn(true);
        when(lambdaSinkConfig.getWhenCondition()).thenReturn(null);
        when(currentBufferPerBatch.getEventCount()).thenReturn(0).thenReturn(1);
        doNothing().when(requestCodec).start(any(), eq(event), any());
        doNothing().when(requestCodec).writeEvent(eq(event), any());
        doNothing().when(currentBufferPerBatch).addRecord(eq(record));
        when(currentBufferPerBatch.getEventCount()).thenReturn(1);
        when(currentBufferPerBatch.getSize()).thenReturn(100L);
        when(currentBufferPerBatch.getDuration()).thenReturn(Duration.ofMillis(500));
        CompletableFuture<InvokeResponse> future = CompletableFuture.completedFuture(invokeResponse);
        when(currentBufferPerBatch.flushToLambda(any())).thenReturn(future);
        when(invokeResponse.statusCode()).thenReturn(202);
        when(lambdaCommonHandler.checkStatusCode(any())).thenReturn(true);
        doNothing().when(lambdaLatencyMetric).record(any(Duration.class));

        lambdaSinkService.output(records);

        verify(currentBufferPerBatch, times(1)).addRecord(eq(record));
        verify(currentBufferPerBatch, times(1)).flushToLambda(any());
        verify(lambdaCommonHandler, times(1)).checkStatusCode(eq(invokeResponse));
        verify(numberOfRecordsSuccessCounter, times(1)).increment(1.0);
    }

    @Test
    public void testHandleFailure_WithDlq() {
        Throwable throwable = new RuntimeException("Test Exception");
        SdkBytes payload = SdkBytes.fromUtf8String("test payload");
        when(currentBufferPerBatch.getEventCount()).thenReturn(1);
        when(currentBufferPerBatch.getPayload()).thenReturn(payload);

        lambdaSinkService.handleFailure(throwable, currentBufferPerBatch);

        verify(numberOfRecordsFailedCounter, times(1)).increment(1.0);
        verify(dlqPushHandler, times(1)).perform(eq(pluginSetting), any(LambdaSinkFailedDlqData.class));
    }

    @Test
    public void testHandleFailure_WithoutDlq() {
        setPrivateField(lambdaSinkService, "dlqPushHandler", null);
        Throwable throwable = new RuntimeException("Test Exception");
        when(currentBufferPerBatch.getEventCount()).thenReturn(1);

        lambdaSinkService.handleFailure(throwable, currentBufferPerBatch);

        verify(numberOfRecordsFailedCounter, times(1)).increment(1);
        verify(dlqPushHandler, never()).perform(any(), any());
    }

    @Test
    public void testOutput_ExceptionDuringProcessing() throws Exception {
        // Arrange
        Record<Event> record = new Record<>(event);
        Collection<Record<Event>> records = Collections.singletonList(record);

        // Mock whenCondition evaluation
        when(expressionEvaluator.evaluateConditional(anyString(), eq(event))).thenReturn(true);
        when(lambdaSinkConfig.getWhenCondition()).thenReturn(null);

        // Mock event handling to throw exception when writeEvent is called
        when(currentBufferPerBatch.getEventCount()).thenReturn(0);
        doNothing().when(requestCodec).start(any(), eq(event), any());
        doThrow(new IOException("Test IOException")).when(requestCodec).writeEvent(eq(event), any());

        // Mock buffer reset
        doNothing().when(currentBufferPerBatch).reset();

        // Mock flushToLambda to prevent NullPointerException
        CompletableFuture<InvokeResponse> future = CompletableFuture.completedFuture(invokeResponse);
        when(currentBufferPerBatch.flushToLambda(any())).thenReturn(future);

        // Act
        lambdaSinkService.output(records);

        // Assert
        verify(requestCodec, times(1)).start(any(), eq(event), any());
        verify(requestCodec, times(1)).writeEvent(eq(event), any());
        verify(numberOfRecordsFailedCounter, times(1)).increment();
    }


}
