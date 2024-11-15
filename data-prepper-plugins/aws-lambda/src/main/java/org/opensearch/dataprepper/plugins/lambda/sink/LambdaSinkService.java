/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.sink;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodec;
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodecConfig;
import org.opensearch.dataprepper.plugins.lambda.common.LambdaCommonHandler;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.BufferFactory;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.InMemoryBufferFactory;
import org.opensearch.dataprepper.plugins.lambda.common.config.BatchOptions;
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
    private final ExpressionEvaluator expressionEvaluator;
    private final Counter numberOfRecordsSuccessCounter;
    private final Counter numberOfRecordsFailedCounter;
    private final Timer lambdaLatencyMetric;
    private final String invocationType;
    private final BufferFactory bufferFactory;
    private final DlqPushHandler dlqPushHandler;
    private final BatchOptions batchOptions;
    private int maxEvents = 0;
    private ByteCount maxBytes = null;
    private Duration maxCollectionDuration = null;
    private int maxRetries = 0;


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

        reentrantLock = new ReentrantLock();
        functionName = lambdaSinkConfig.getFunctionName();
        maxRetries = lambdaSinkConfig.getMaxConnectionRetries();
        if(lambdaSinkConfig.getBatchOptions() == null){
            batchOptions = new BatchOptions();
        } else {
            batchOptions = lambdaSinkConfig.getBatchOptions();
        }

        maxEvents = batchOptions.getThresholdOptions().getEventCount();
        maxBytes = batchOptions.getThresholdOptions().getMaximumSize();
        maxCollectionDuration = batchOptions.getThresholdOptions().getEventCollectTimeOut();
        invocationType = lambdaSinkConfig.getInvocationType().getAwsLambdaValue();

        this.bufferFactory = bufferFactory;
        LOG.info("LambdaFunctionName:{} , invocationType:{}", functionName, invocationType);
    }


    public void output(Collection<Record<Event>> records) {
        if (records.isEmpty()) {
            return;
        }

        reentrantLock.lock();

        //Result from lambda is not currently processes.
        List<Record<Event>> resultRecords = null;

        BufferFactory bufferFactory = new InMemoryBufferFactory();
        Buffer currentBufferPerBatch = createBuffer(bufferFactory);
        List futureList = new ArrayList<>();

        JsonOutputCodecConfig jsonOutputCodecConfig = new JsonOutputCodecConfig();
        jsonOutputCodecConfig.setKeyName(batchOptions.getKeyName());
        OutputCodec requestCodec = new JsonOutputCodec(jsonOutputCodecConfig);

        try {
            for (Record<Event> record : records) {
                final Event event = record.getData();

                try {
                    if (currentBufferPerBatch.getEventCount() == 0) {
                        requestCodec.start(currentBufferPerBatch.getOutputStream(), event, new OutputCodecContext());
                    }
                    requestCodec.writeEvent(event, currentBufferPerBatch.getOutputStream());
                    currentBufferPerBatch.addRecord(record);

                    flushToLambdaIfNeeded(currentBufferPerBatch, requestCodec, futureList, true); // Force flush remaining events
                } catch (IOException e) {
                    LOG.error("Exception while writing to codec {}", event, e);
                    handleFailure(e, currentBufferPerBatch);
                } catch (Exception e) {
                    LOG.error("Exception while processing event {}", event, e);
                    handleFailure(e, currentBufferPerBatch);
                }
            }
            // Flush any remaining events after processing all records
            if (currentBufferPerBatch.getEventCount() > 0) {
                LOG.info("Force Flushing the remaining {} events in the buffer", currentBufferPerBatch.getEventCount());
                try {
                    flushToLambdaIfNeeded(currentBufferPerBatch, requestCodec, futureList, true); // Force flush remaining events
                } catch (Exception e) {
                    LOG.error("Exception while flushing remaining events", e);
                    handleFailure(e, currentBufferPerBatch);
                }
            }
        } finally {
            reentrantLock.unlock();
        }

        // Wait for all futures to complete
        LambdaCommonHandler.waitForFutures(futureList);

        // Release event handles for records not sent to Lambda
        for (Record<Event> record : records) {
            Event event = record.getData();
            releaseEventHandle(event, true);
        }
    }

    void flushToLambdaIfNeeded(Buffer currentBufferPerBatch, OutputCodec requestCodec,
                               List futureList, boolean forceFlush) {
        if (forceFlush || ThresholdCheck.checkThresholdExceed(currentBufferPerBatch, maxEvents, maxBytes, maxCollectionDuration)) {
            try {
                requestCodec.complete(currentBufferPerBatch.getOutputStream());

                // Capture buffer before resetting
                final Buffer flushedBuffer = currentBufferPerBatch;

                CompletableFuture<InvokeResponse> future = flushedBuffer.flushToLambda(invocationType);

                // Handle future
                CompletableFuture<Void> processingFuture = future.thenAccept(response -> {
                    handleLambdaResponse(flushedBuffer, response);
                }).exceptionally(throwable -> {
                    // Failure handler
                    List<Record<Event>> bufferRecords = flushedBuffer.getRecords();
                    Record<Event> eventRecord = bufferRecords.isEmpty() ? null : bufferRecords.get(0);
                    LOG.error(NOISY, "Exception occurred while invoking Lambda. Function: {} , Event: {}",
                            functionName, eventRecord == null? "null":eventRecord.getData(), throwable);
                    requestPayloadMetric.set(flushedBuffer.getPayloadRequestSize());
                    responsePayloadMetric.set(0);
                    Duration latency = flushedBuffer.stopLatencyWatch();
                    lambdaLatencyMetric.record(latency.toMillis(), TimeUnit.MILLISECONDS);
                    handleFailure(throwable, flushedBuffer);
                    return null;
                });

                futureList.add(processingFuture);

                // Create a new buffer for the next batch
                currentBufferPerBatch = createBuffer(bufferFactory);
            } catch (IOException e) {
                LOG.error("Exception while flushing to lambda", e);
                handleFailure(e, currentBufferPerBatch);
                currentBufferPerBatch = createBuffer(bufferFactory);
            }
        }
    }

    void handleFailure(Throwable throwable, Buffer flushedBuffer) {
        try {
            if (flushedBuffer.getEventCount() > 0) {
                numberOfRecordsFailedCounter.increment(flushedBuffer.getEventCount());
            }

            SdkBytes payload = flushedBuffer.getPayload();
            if (dlqPushHandler != null) {
                dlqPushHandler.perform(pluginSetting, new LambdaSinkFailedDlqData(payload, throwable.getMessage(), 0));
                releaseEventHandlesPerBatch(true, flushedBuffer);
            } else {
                releaseEventHandlesPerBatch(false, flushedBuffer);
            }
        } catch (Exception ex){
            LOG.error("Exception occured during error handling");
        }
    }

    /*
     * Release events per batch
     */
    private void releaseEventHandlesPerBatch(boolean success, Buffer flushedBuffer) {
        List<Record<Event>> records = flushedBuffer.getRecords();
        for (Record<Event> record : records) {
            Event event = record.getData();
            releaseEventHandle(event, success);
        }
    }

    /**
     * Releases the event handle based on processing success.
     *
     * @param event   the event to release
     * @param success indicates if processing was successful
     */
    private void releaseEventHandle(Event event, boolean success) {
        if (event != null) {
            EventHandle eventHandle = event.getEventHandle();
            if (eventHandle != null) {
                eventHandle.release(success);
            }
        }
    }

    private void handleLambdaResponse(Buffer flushedBuffer, InvokeResponse response) {
        boolean success = LambdaCommonHandler.checkStatusCode(response);
        if (success) {
            LOG.info("Successfully flushed {} events", flushedBuffer.getEventCount());
            SdkBytes payload = response.payload();
            if (payload == null || payload.asByteArray() == null || payload.asByteArray().length == 0) {
                responsePayloadMetric.set(0);
            } else {
                responsePayloadMetric.set(payload.asByteArray().length);
            }
            //metrics
            requestPayloadMetric.set(flushedBuffer.getPayloadRequestSize());
            numberOfRecordsSuccessCounter.increment(flushedBuffer.getSize());
            Duration latency = flushedBuffer.stopLatencyWatch();
            lambdaLatencyMetric.record(latency.toMillis(), TimeUnit.MILLISECONDS);
            }
        else {
            // Non-2xx status code treated as failure
            handleFailure(new RuntimeException("Non-success Lambda status code: " + response.statusCode()), flushedBuffer);
        }
    }

    Buffer createBuffer(BufferFactory bufferFactory){
        try {
            return bufferFactory.getBuffer(lambdaAsyncClient, functionName, invocationType);
        } catch (IOException e) {
            LOG.error("Failed to create new buffer");
            throw new RuntimeException(e);
        }
    }

}
