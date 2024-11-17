/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.sink;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.record.RecordMetadata;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodec;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.InMemoryBuffer;
import org.opensearch.dataprepper.plugins.lambda.common.config.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.BatchOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.ClientOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.InvocationType;
import org.opensearch.dataprepper.plugins.lambda.common.config.ThresholdOptions;
import org.opensearch.dataprepper.plugins.lambda.sink.dlq.DlqPushHandler;
import org.opensearch.dataprepper.plugins.lambda.sink.dlq.LambdaSinkFailedDlqData;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

public class LambdaSinkTest {

  @Mock
  SinkContext sinkContext;
  @Mock
  private LambdaAsyncClient lambdaAsyncClient;
  @Mock
  private LambdaSinkConfig lambdaSinkConfig;
  @Mock
  private PluginMetrics pluginMetrics;
  @Mock
  private PluginFactory pluginFactory;

  private PluginSetting pluginSetting;
  @Mock
  private OutputCodecContext codecContext;
  @Mock
  private AwsCredentialsSupplier awsCredentialsSupplier;
  @Mock
  private DlqPushHandler dlqPushHandler;
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
  private Event event;
  @Mock
  private EventHandle eventHandle;
  @Mock
  private EventMetadata eventMetadata;
  @Mock
  private InvokeResponse invokeResponse;

  private LambdaSink lambdaSink;

  @Mock
  private AwsAuthenticationOptions awsAuthenticationOptions;

  public static Record<Event> getSampleRecord() {
    Event event = JacksonEvent.fromMessage(UUID.randomUUID().toString());
    return new Record<>(event, RecordMetadata.defaultMetadata());
  }

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    // Mock PluginMetrics counters and timers
    when(pluginMetrics.counter("lambdaSinkObjectsEventsSucceeded")).thenReturn(
        numberOfRecordsSuccessCounter);
    when(pluginMetrics.counter("lambdaSinkObjectsEventsFailed")).thenReturn(
        numberOfRecordsFailedCounter);
    when(pluginMetrics.timer(anyString())).thenReturn(lambdaLatencyMetric);
    when(pluginMetrics.gauge(anyString(), any(AtomicLong.class))).thenReturn(new AtomicLong());

    // Mock lambdaSinkConfig
    when(lambdaSinkConfig.getFunctionName()).thenReturn("test-function");
    when(lambdaSinkConfig.getInvocationType()).thenReturn(InvocationType.EVENT);

    // Mock BatchOptions and ThresholdOptions
    BatchOptions batchOptions = mock(BatchOptions.class);
    ThresholdOptions thresholdOptions = mock(ThresholdOptions.class);
    when(batchOptions.getKeyName()).thenReturn("test");
    when(lambdaSinkConfig.getBatchOptions()).thenReturn(batchOptions);
    when(batchOptions.getThresholdOptions()).thenReturn(thresholdOptions);
    when(thresholdOptions.getEventCount()).thenReturn(10);
    when(thresholdOptions.getMaximumSize()).thenReturn(ByteCount.parse("1mb"));
    when(thresholdOptions.getEventCollectTimeOut()).thenReturn(Duration.ofSeconds(1));

    // Mock JsonOutputCodec
    requestCodec = mock(JsonOutputCodec.class);
    when(pluginFactory.loadPlugin(eq(OutputCodec.class), any(PluginSetting.class))).thenReturn(
        requestCodec);

    // Initialize bufferFactory and buffer
    currentBufferPerBatch = mock(Buffer.class);
    when(currentBufferPerBatch.getEventCount()).thenReturn(0);
    when(lambdaSinkConfig.getAwsAuthenticationOptions()).thenReturn(awsAuthenticationOptions);
    when(awsAuthenticationOptions.getAwsRegion()).thenReturn(Region.of("us-east-1"));
    this.pluginSetting = new PluginSetting("aws_lambda", Collections.emptyMap());
    this.pluginSetting.setPipelineName(UUID.randomUUID().toString());
    this.awsAuthenticationOptions = new AwsAuthenticationOptions();

    ClientOptions clientOptions = new ClientOptions();
    when(lambdaSinkConfig.getClientOptions()).thenReturn(clientOptions);

    this.lambdaSink = new LambdaSink(pluginSetting, lambdaSinkConfig, pluginFactory, sinkContext,
        awsCredentialsSupplier, expressionEvaluator);
  }

    /*
    @Test
    public void testOutput_SuccessfulProcessing() throws Exception {
        Event event = mock(Event.class);
        Record<Event> record = new Record<>(event);
        Collection<Record<Event>> records = Collections.singletonList(record);

        when(expressionEvaluator.evaluateConditional(anyString(), eq(event))).thenReturn(true);
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

     */

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
  public void testHandleFailure_WithDlq() {
    Throwable throwable = new RuntimeException("Test Exception");
    Buffer buffer = new InMemoryBuffer(UUID.randomUUID().toString());
    buffer.addRecord(getSampleRecord());
    setPrivateField(lambdaSink, "dlqPushHandler", dlqPushHandler);
    setPrivateField(lambdaSink, "numberOfRecordsFailedCounter", numberOfRecordsFailedCounter);
    lambdaSink.handleFailure(throwable, buffer);
    verify(numberOfRecordsFailedCounter, times(1)).increment(1.0);
    verify(dlqPushHandler, times(1)).perform(eq(pluginSetting), any(LambdaSinkFailedDlqData.class));
  }

  @Test
  public void testHandleFailure_WithoutDlq() {
    Throwable throwable = new RuntimeException("Test Exception");
    Buffer buffer = new InMemoryBuffer(UUID.randomUUID().toString());
    buffer.addRecord(getSampleRecord());
    when(lambdaSinkConfig.getDlqPluginSetting()).thenReturn(null);
    setPrivateField(lambdaSink, "numberOfRecordsFailedCounter", numberOfRecordsFailedCounter);
    lambdaSink.handleFailure(throwable, buffer);
    verify(numberOfRecordsFailedCounter, times(1)).increment(1);
    verify(dlqPushHandler, never()).perform(any(), any());
  }

    /*
    @Test
    public void testOutput_ExceptionDuringProcessing() throws Exception {
        // Arrange
        Record<Event> record = new Record<>(event);
        Collection<Record<Event>> records = Collections.singletonList(record);

        // Mock whenCondition evaluation
        when(expressionEvaluator.evaluateConditional(anyString(), eq(event))).thenReturn(true);

        // Mock event handling to throw exception when writeEvent is called
        when(currentBufferPerBatch.getEventCount()).thenReturn(0).thenReturn(1);
        when(lambdaCommonHandler.checkStatusCode(any())).thenReturn(true);
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
        verify(numberOfRecordsFailedCounter, times(1)).increment(1);
    }
     */


}
