/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.sink;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodec;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.InMemoryBuffer;
import org.opensearch.dataprepper.plugins.lambda.common.config.AwsAuthenticationOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.BatchOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.ClientOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.InvocationType;
import org.opensearch.dataprepper.plugins.lambda.common.config.ThresholdOptions;
import org.opensearch.dataprepper.plugins.lambda.common.LambdaCommonHandler;
import org.opensearch.dataprepper.plugins.lambda.sink.dlq.DlqPushHandler;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import org.opensearch.dataprepper.plugins.dlq.DlqProvider;
import org.opensearch.dataprepper.plugins.dlq.DlqWriter;
import software.amazon.awssdk.regions.Region;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.lambda.utils.LambdaTestSetupUtil.getSampleRecord;

public class LambdaSinkTest {

    private static final String TEST_BUCKET = "test";
    private static final String TEST_ROLE = "arn:aws:iam::524239988122:role/app-test";
    private static final String TEST_REGION = "ap-south-1";

    @Mock
    SinkContext sinkContext;

    @Mock
    private LambdaSinkConfig lambdaSinkConfig;
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private PluginFactory pluginFactory;

    private PluginSetting pluginSetting;

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
    private DistributionSummary responsePayloadMetric;
    @Mock
    private DistributionSummary lambdaPayloadMetric;
    @Mock
    private OutputCodec requestCodec;
    @Mock
    private Buffer currentBufferPerBatch;
    @Mock
    private PluginModel dlqConfig;
    @Mock
    private DlqProvider dlqProvider;
    @Mock
    private DlqWriter dlqWriter;
    @Mock
    CompletableFuture<InvokeResponse> future;
    @Mock
    InvokeResponse response;

    private LambdaSink lambdaSink;

    @Mock
    private AwsAuthenticationOptions awsAuthenticationOptions;


    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        // Mock PluginMetrics counters and timers
        when(pluginMetrics.counter(LambdaSink.NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_SUCCESS)).thenReturn(
                numberOfRecordsSuccessCounter);
        when(pluginMetrics.counter(LambdaSink.NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_FAILED)).thenReturn(
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

        dlqConfig = mock(PluginModel.class);
        dlqWriter = mock(DlqWriter.class);
        dlqProvider = mock(DlqProvider.class);
        when(dlqConfig.getPluginName()).thenReturn("testPlugin");
        when(dlqConfig.getPluginSettings()).thenReturn(Map.of("bucket", TEST_BUCKET, DlqPushHandler.REGION, TEST_REGION, DlqPushHandler.STS_ROLE_ARN, TEST_ROLE));
        when(lambdaSinkConfig.getDlq()).thenReturn(dlqConfig);
        when(dlqProvider.getDlqWriter(anyString())).thenReturn(Optional.of(dlqWriter));
        when(pluginFactory.loadPlugin(eq(DlqProvider.class), any(PluginSetting.class))).thenReturn(dlqProvider);
        this.lambdaSink = new LambdaSink(pluginSetting, lambdaSinkConfig, pluginFactory, sinkContext,
                awsCredentialsSupplier, expressionEvaluator);
    }

    @Test
    public void testOutput_SuccessfulProcessing() throws Exception {
        try ( MockedStatic<LambdaCommonHandler> lambdaCommonHandler = Mockito.mockStatic(LambdaCommonHandler.class)){
            Event event = mock(Event.class);
            Record<Event> record = new Record<>(event);
            Collection<Record<Event>> records = Collections.singletonList(record);

            future = mock(CompletableFuture.class);
            response = mock(InvokeResponse.class);
            when(expressionEvaluator.evaluateConditional(anyString(), eq(event))).thenReturn(true);
            when(response.statusCode()).thenReturn(202);
            when(response.payload()).thenReturn(SdkBytes.fromUtf8String("{\"k\":\"v\"}"));
            when(future.join()).thenReturn(response);

            setPrivateField(lambdaSink, "numberOfRecordsSuccessCounter", numberOfRecordsSuccessCounter);
            setPrivateField(lambdaSink, "numberOfRecordsFailedCounter", numberOfRecordsFailedCounter);

            doNothing().when(currentBufferPerBatch).addRecord(eq(record));
            when(currentBufferPerBatch.getRecords()).thenReturn(new ArrayList<>(records));
            when(currentBufferPerBatch.getEventCount()).thenReturn(1);
            when(currentBufferPerBatch.getSize()).thenReturn(100L);
            when(currentBufferPerBatch.getPayloadRequestSize()).thenReturn(100L);
            when(currentBufferPerBatch.getDuration()).thenReturn(Duration.ofMillis(500));
            doNothing().when(lambdaLatencyMetric).record(any(Duration.class));
            doNothing().when(lambdaPayloadMetric).record(any(Double.class));
            doNothing().when(responsePayloadMetric).record(any(Double.class));
            lambdaCommonHandler.when(() ->
                    LambdaCommonHandler.sendRecords(anyList(), any(LambdaSinkConfig.class), any(LambdaAsyncClient.class), any(OutputCodecContext.class))).thenReturn(Map.of(currentBufferPerBatch, future));

            lambdaCommonHandler.when(() ->
                    LambdaCommonHandler.isSuccess(any(InvokeResponse.class))).thenReturn(true);
            lambdaSink.doOutput(records);

            verify(numberOfRecordsSuccessCounter, times(1)).increment(1.0);
        }
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
    public void testOutput_ExceptionDuringProcessing() throws Exception {
        try ( MockedStatic<LambdaCommonHandler> lambdaCommonHandler = Mockito.mockStatic(LambdaCommonHandler.class)) {
            Event event = mock(Event.class);
            Record<Event> record = new Record<>(event);
            Collection<Record<Event>> records = Collections.singletonList(record);

            future = mock(CompletableFuture.class);
            response = mock(InvokeResponse.class);
            when(expressionEvaluator.evaluateConditional(anyString(), eq(event))).thenReturn(true);
            when(response.statusCode()).thenReturn(202);
            when(response.payload()).thenReturn(SdkBytes.fromUtf8String("{\"k\":\"v\"}"));
            when(future.join()).thenThrow(new RuntimeException("Test Exception"));

            setPrivateField(lambdaSink, "numberOfRecordsSuccessCounter", numberOfRecordsSuccessCounter);
            setPrivateField(lambdaSink, "numberOfRecordsFailedCounter", numberOfRecordsFailedCounter);

            doNothing().when(currentBufferPerBatch).addRecord(eq(record));
            when(currentBufferPerBatch.getRecords()).thenReturn(new ArrayList<>(records));
            when(currentBufferPerBatch.getEventCount()).thenReturn(1);
            when(currentBufferPerBatch.getSize()).thenReturn(100L);
            when(currentBufferPerBatch.getPayloadRequestSize()).thenReturn(100L);
            when(currentBufferPerBatch.getDuration()).thenReturn(Duration.ofMillis(500));
            doNothing().when(lambdaLatencyMetric).record(any(Duration.class));
            doNothing().when(lambdaPayloadMetric).record(any(Double.class));
            doNothing().when(responsePayloadMetric).record(any(Double.class));
            lambdaCommonHandler.when(() ->
                    LambdaCommonHandler.sendRecords(anyList(), any(LambdaSinkConfig.class), any(LambdaAsyncClient.class), any(OutputCodecContext.class))).thenReturn(Map.of(currentBufferPerBatch, future));

            lambdaCommonHandler.when(() ->
                    LambdaCommonHandler.isSuccess(any(InvokeResponse.class))).thenReturn(true);
            lambdaSink.doOutput(records);

            verify(numberOfRecordsFailedCounter, times(1)).increment(1);
        }
    }



}
