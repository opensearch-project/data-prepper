/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.sink;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodec;
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodecConfig;
import org.opensearch.dataprepper.plugins.lambda.common.LambdaCommonHandler;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.BufferFactory;
import org.opensearch.dataprepper.plugins.lambda.common.config.BatchOptions;
import static org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig.EVENT;
import static org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig.invocationTypeMap;
import org.opensearch.dataprepper.plugins.lambda.common.util.ThresholdCheck;
import org.opensearch.dataprepper.plugins.lambda.sink.dlq.DlqPushHandler;
import org.opensearch.dataprepper.plugins.lambda.sink.dlq.LambdaSinkFailedDlqData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LambdaSinkService {

    public static final String NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_SUCCESS = "lambdaSinkObjectsEventsSucceeded";
    public static final String NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_FAILED = "lambdaSinkObjectsEventsFailed";
    public static final String LAMBDA_LATENCY_METRIC = "lambdaSinkLatency";
    public static final String REQUEST_PAYLOAD_SIZE = "requestPayloadSize";
    public static final String RESPONSE_PAYLOAD_SIZE = "responsePayloadSize";
    private static final Logger LOG = LoggerFactory.getLogger(LambdaSinkService.class);
    private final AtomicLong requestPayloadMetric;
    private final AtomicLong responsePayloadMetric;
    private final PluginSetting pluginSetting;
    private final Lock reentrantLock;
    private final LambdaSinkConfig lambdaSinkConfig;
    private final LambdaAsyncClient lambdaAsyncClient;
    private final String functionName;
    private final String whenCondition;
    private final ExpressionEvaluator expressionEvaluator;
    private final Counter numberOfRecordsSuccessCounter;
    private final Counter numberOfRecordsFailedCounter;
    private final Timer lambdaLatencyMetric;
    private final String invocationType;
    private final BufferFactory bufferFactory;
    private final DlqPushHandler dlqPushHandler;
    private final List<Event> events;
    private final BatchOptions batchOptions;
    private int maxEvents = 0;
    private ByteCount maxBytes = null;
    private Duration maxCollectionDuration = null;
    private int maxRetries = 0;
    private OutputCodec requestCodec = null;
    private OutputCodecContext codecContext = null;
    private final LambdaCommonHandler lambdaCommonHandler;
    private Buffer currentBufferPerBatch = null;
    List<CompletableFuture<Void>> futureList;


    public LambdaSinkService(final LambdaAsyncClient lambdaAsyncClient, final LambdaSinkConfig lambdaSinkConfig, final PluginMetrics pluginMetrics, final PluginFactory pluginFactory, final PluginSetting pluginSetting, final OutputCodecContext codecContext, final AwsCredentialsSupplier awsCredentialsSupplier, final DlqPushHandler dlqPushHandler, final BufferFactory bufferFactory, final ExpressionEvaluator expressionEvaluator) {
        this.lambdaSinkConfig = lambdaSinkConfig;
        this.pluginSetting = pluginSetting;
        this.expressionEvaluator = expressionEvaluator;
        this.dlqPushHandler = dlqPushHandler;
        this.lambdaAsyncClient = lambdaAsyncClient;
        this.numberOfRecordsSuccessCounter = pluginMetrics.counter(NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_SUCCESS);
        this.numberOfRecordsFailedCounter = pluginMetrics.counter(NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_FAILED);
        this.lambdaLatencyMetric = pluginMetrics.timer(LAMBDA_LATENCY_METRIC);
        this.requestPayloadMetric = pluginMetrics.gauge(REQUEST_PAYLOAD_SIZE, new AtomicLong());
        this.responsePayloadMetric = pluginMetrics.gauge(RESPONSE_PAYLOAD_SIZE, new AtomicLong());
        this.codecContext = codecContext;

        reentrantLock = new ReentrantLock();
        functionName = lambdaSinkConfig.getFunctionName();
        maxRetries = lambdaSinkConfig.getMaxConnectionRetries();
        batchOptions = lambdaSinkConfig.getBatchOptions();
        whenCondition = lambdaSinkConfig.getWhenCondition();

        JsonOutputCodecConfig jsonOutputCodecConfig = new JsonOutputCodecConfig();
        jsonOutputCodecConfig.setKeyName(batchOptions.getKeyName());
        requestCodec = new JsonOutputCodec(jsonOutputCodecConfig);

        maxEvents = batchOptions.getThresholdOptions().getEventCount();
        maxBytes = batchOptions.getThresholdOptions().getMaximumSize();
        maxCollectionDuration = batchOptions.getThresholdOptions().getEventCollectTimeOut();
        invocationType = invocationTypeMap.get(lambdaSinkConfig.getInvocationType());
        if(!lambdaSinkConfig.getInvocationType().equals(EVENT)){
            throw new RuntimeException("Only Event invocation type is supported for sink");
        }
        events = new ArrayList();
        futureList = new ArrayList<>();

        this.bufferFactory = bufferFactory;

        LOG.info("LambdaFunctionName:{} , invocationType:{}", functionName, invocationType);
        // Initialize LambdaCommonHandler
        lambdaCommonHandler = new LambdaCommonHandler(LOG, lambdaAsyncClient, functionName, invocationType, bufferFactory);
    }


    public void output(Collection<Record<Event>> records) {
        if (records.isEmpty()) {
            return;
        }

        List<Record<Event>> resultRecords = new ArrayList<>();
        reentrantLock.lock();
        try {
            for (Record<Event> record : records) {
                final Event event = record.getData();

                if (whenCondition != null && !expressionEvaluator.evaluateConditional(whenCondition, event)) {
                    resultRecords.add(record);
                    continue;
                }
                try {
                    if (currentBufferPerBatch.getEventCount() == 0) {
                        requestCodec.start(currentBufferPerBatch.getOutputStream(), event, codecContext);
                    }
                    requestCodec.writeEvent(event, currentBufferPerBatch.getOutputStream());
                    currentBufferPerBatch.addRecord(record);

                    flushToLambdaIfNeeded(resultRecords, false);
                } catch (Exception e) {
                    LOG.error("Exception while processing event {}", event, e);
                    handleFailure(e, currentBufferPerBatch);
                    currentBufferPerBatch.reset();
                }
            }
            // Flush any remaining events after processing all records
            if (currentBufferPerBatch.getEventCount() > 0) {
                LOG.info("Force Flushing the remaining {} events in the buffer", currentBufferPerBatch.getEventCount());
                try {
                    flushToLambdaIfNeeded(resultRecords, true); // Force flush remaining events
                } catch (Exception e) {
                    LOG.error("Exception while flushing remaining events", e);
                    handleFailure(e, currentBufferPerBatch);
                }
            }
        } finally {
            reentrantLock.unlock();
        }

        // Wait for all futures to complete
        lambdaCommonHandler.waitForFutures(futureList);

    }

    void flushToLambdaIfNeeded(List<Record<Event>> resultRecords, boolean forceFlush) {
        if (forceFlush || ThresholdCheck.checkThresholdExceed(currentBufferPerBatch, maxEvents, maxBytes, maxCollectionDuration)) {
            try {
                requestCodec.complete(currentBufferPerBatch.getOutputStream());

                // Capture buffer before resetting
                final Buffer flushedBuffer = currentBufferPerBatch;
                final int eventCount = currentBufferPerBatch.getEventCount();

                CompletableFuture<InvokeResponse> future = flushedBuffer.flushToLambda(invocationType);

                // Handle future
                CompletableFuture<Void> processingFuture = future.thenAccept(response -> {
                    // Success handler
                    lambdaCommonHandler.checkStatusCode(response);
                    LOG.info("Successfully flushed {} events", eventCount);
                    numberOfRecordsSuccessCounter.increment(eventCount);
                    requestPayloadMetric.set(flushedBuffer.getPayloadRequestSize());
                    Duration latency = flushedBuffer.stopLatencyWatch();
                    lambdaLatencyMetric.record(latency.toMillis(), TimeUnit.MILLISECONDS);
                }).exceptionally(throwable -> {
                    // Failure handler
                    LOG.error("Exception occurred while invoking Lambda. Function: {}, event in batch:{} | Exception: ", functionName, currentBufferPerBatch.getRecords().get(0), throwable);
                    requestPayloadMetric.set(flushedBuffer.getPayloadRequestSize());
                    responsePayloadMetric.set(0);
                    Duration latency = flushedBuffer.stopLatencyWatch();
                    lambdaLatencyMetric.record(latency.toMillis(), TimeUnit.MILLISECONDS);
                    handleFailure(throwable, flushedBuffer);
                    return null;
                });

                futureList.add(processingFuture);

                // Create a new buffer for the next batch
                currentBufferPerBatch = lambdaCommonHandler.createBuffer(currentBufferPerBatch);
            } catch (IOException e) {
                LOG.error("Exception while flushing to lambda", e);
                handleFailure(e, currentBufferPerBatch);
                currentBufferPerBatch = lambdaCommonHandler.createBuffer(currentBufferPerBatch);
            }
        }
    }

    void handleFailure(Throwable throwable, Buffer flushedBuffer) {
        numberOfRecordsFailedCounter.increment(currentBufferPerBatch.getEventCount());
            SdkBytes payload = currentBufferPerBatch.getPayload();
            if (dlqPushHandler != null) {
                dlqPushHandler.perform(pluginSetting, new LambdaSinkFailedDlqData(payload, throwable.getMessage(), 0));
                lambdaCommonHandler.releaseEventHandlesPerBatch(true, flushedBuffer);
            } else {
                lambdaCommonHandler.releaseEventHandlesPerBatch(false, flushedBuffer);
            }
    }
}
