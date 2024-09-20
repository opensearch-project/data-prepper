/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.sink;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.EVENT;
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
import org.opensearch.dataprepper.plugins.codec.json.NdjsonOutputCodec;
import org.opensearch.dataprepper.plugins.codec.json.NdjsonOutputConfig;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.BufferFactory;
import org.opensearch.dataprepper.plugins.lambda.common.config.BatchOptions;
import static org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig.BATCH_EVENT;
import static org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig.SINGLE_EVENT;
import static org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig.invocationTypeMap;
import org.opensearch.dataprepper.plugins.lambda.common.util.ThresholdCheck;
import org.opensearch.dataprepper.plugins.lambda.sink.dlq.DlqPushHandler;
import org.opensearch.dataprepper.plugins.lambda.sink.dlq.LambdaSinkFailedDlqData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
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
    private final LambdaClient lambdaClient;
    private final String functionName;
    private final String whenCondition;
    private final ExpressionEvaluator expressionEvaluator;
    private final Counter numberOfRecordsSuccessCounter;
    private final Counter numberOfRecordsFailedCounter;
    private final Timer lambdaLatencyMetric;
    private final String invocationType;
    private final BufferFactory bufferFactory;
    private final DlqPushHandler dlqPushHandler;
    private final Collection<EventHandle> bufferedEventHandles;
    private final List<Event> events;
    private final BatchOptions batchOptions;
    private final Boolean isBatchEnabled;
    private int maxEvents = 0;
    private ByteCount maxBytes = null;
    private Duration maxCollectionDuration = null;
    private int maxRetries = 0;
    private Buffer currentBuffer;
    private OutputCodec codec = null;
    private OutputCodecContext codecContext = null;
    private String payloadModel = null;

    public LambdaSinkService(final LambdaClient lambdaClient, final LambdaSinkConfig lambdaSinkConfig, final PluginMetrics pluginMetrics, final PluginFactory pluginFactory, final PluginSetting pluginSetting, final OutputCodecContext codecContext, final AwsCredentialsSupplier awsCredentialsSupplier, final DlqPushHandler dlqPushHandler, final BufferFactory bufferFactory, final ExpressionEvaluator expressionEvaluator) {
        this.lambdaSinkConfig = lambdaSinkConfig;
        this.pluginSetting = pluginSetting;
        this.expressionEvaluator = expressionEvaluator;
        this.dlqPushHandler = dlqPushHandler;
        this.lambdaClient = lambdaClient;
        this.numberOfRecordsSuccessCounter = pluginMetrics.counter(NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_SUCCESS);
        this.numberOfRecordsFailedCounter = pluginMetrics.counter(NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_FAILED);
        this.lambdaLatencyMetric = pluginMetrics.timer(LAMBDA_LATENCY_METRIC);
        this.requestPayloadMetric = pluginMetrics.gauge(REQUEST_PAYLOAD_SIZE, new AtomicLong());
        this.responsePayloadMetric = pluginMetrics.gauge(RESPONSE_PAYLOAD_SIZE, new AtomicLong());
        reentrantLock = new ReentrantLock();
        functionName = lambdaSinkConfig.getFunctionName();
        payloadModel = lambdaSinkConfig.getPayloadModel();
        maxRetries = lambdaSinkConfig.getMaxConnectionRetries();
        batchOptions = lambdaSinkConfig.getBatchOptions();
        whenCondition = lambdaSinkConfig.getWhenCondition();

        if (payloadModel.equals(BATCH_EVENT)) {
            JsonOutputCodecConfig jsonOutputCodecConfig = new JsonOutputCodecConfig();
            jsonOutputCodecConfig.setKeyName(batchOptions.getKeyName());
            codec = new JsonOutputCodec(jsonOutputCodecConfig);
            maxEvents = batchOptions.getThresholdOptions().getEventCount();
            maxBytes = batchOptions.getThresholdOptions().getMaximumSize();
            maxCollectionDuration = batchOptions.getThresholdOptions().getEventCollectTimeOut();
            isBatchEnabled = true;
        } else if (payloadModel.equals(SINGLE_EVENT)) {
            NdjsonOutputConfig ndjsonOutputCodecConfig = new NdjsonOutputConfig();
            codec = new NdjsonOutputCodec(ndjsonOutputCodecConfig);
            isBatchEnabled = false;
        } else {
            throw new RuntimeException("invalid payload_model option");
        }
        this.codecContext = codecContext;
        bufferedEventHandles = new LinkedList<>();
        events = new ArrayList();

        invocationType = invocationTypeMap.get(lambdaSinkConfig.getInvocationType());

        this.bufferFactory = bufferFactory;
        try {
            currentBuffer = bufferFactory.getBuffer(lambdaClient, functionName, invocationType);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void output(Collection<Record<Event>> records) {
        if (records.isEmpty() && currentBuffer.getEventCount() == 0) {
            return;
        }
        List<Event> failedEvents = new ArrayList<>();
        Exception sampleException = null;
        reentrantLock.lock();
        try {
            for (Record<Event> record : records) {
                final Event event = record.getData();

                if (whenCondition != null && !expressionEvaluator.evaluateConditional(whenCondition, event)) {
                    continue;
                }

                try {
                    if (currentBuffer.getEventCount() == 0) {
                        codec.start(currentBuffer.getOutputStream(), event, codecContext);
                    }
                    codec.writeEvent(event, currentBuffer.getOutputStream());
                    int count = currentBuffer.getEventCount() + 1;
                    currentBuffer.setEventCount(count);

                    bufferedEventHandles.add(event.getEventHandle());
                    flushToLambdaIfNeeded();
                } catch (Exception ex) {
                    LOG.error(EVENT, "There was an exception while processing Event [{}]" , event, ex);
                    if (sampleException == null) {
                        sampleException = ex;
                    }
                    failedEvents.add(event);
                    handleFailure(currentBuffer, ex);                //reset buffer
                    try {
                        currentBuffer = bufferFactory.getBuffer(lambdaClient, functionName, invocationType);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        } finally {
            reentrantLock.unlock();
        }

        if (!failedEvents.isEmpty()) {
            failedEvents.stream().map(Event::getEventHandle).forEach(eventHandle -> eventHandle.release(false));
            LOG.error("Unable to add {} events to buffer. Dropping these events. Sample exception provided.", failedEvents.size(), sampleException);
        }
    }

    private void releaseEventHandles(final boolean result) {
        for (EventHandle eventHandle : bufferedEventHandles) {
            eventHandle.release(result);
        }
        bufferedEventHandles.clear();
    }

    private void flushToLambdaIfNeeded() {
        LOG.info("Flush to Lambda check: currentBuffer.size={}, currentBuffer.events={}, currentBuffer.duration={}", currentBuffer.getSize(), currentBuffer.getEventCount(), currentBuffer.getDuration());

        if (ThresholdCheck.checkThresholdExceed(currentBuffer, maxEvents, maxBytes, maxCollectionDuration, isBatchEnabled)) {
            try {
                codec.complete(currentBuffer.getOutputStream());
                responsePayloadMetric.set(currentBuffer.getPayloadResponseSyncSize());
                InvokeResponse response = currentBuffer.flushToLambdaAsync();
                handleLambdaResponse(response);
                lambdaLatencyMetric.record(currentBuffer.getFlushLambdaSyncLatencyMetric());
                requestPayloadMetric.set(currentBuffer.getPayloadRequestSyncSize());
                numberOfRecordsSuccessCounter.increment(currentBuffer.getEventCount());
                releaseEventHandles(true);
            } catch (AwsServiceException | SdkClientException e) {
                LOG.error("Exception occurred while uploading records to lambda  : {} | exception:", functionName, e);
                handleFailure(currentBuffer, e);
            } catch (final IOException e) {
                LOG.error("Exception while completing codec", e);
                handleFailure(currentBuffer, e);
            }
            // Reset buffer
            try {
                currentBuffer = bufferFactory.getBuffer(lambdaClient, functionName, invocationType);
            } catch (IOException ex) {
                throw new RuntimeException("Failed to reset buffer after exception", ex);
            }
        }
    }

    private void handleFailure(Buffer currentBuffer, Exception e) {
        numberOfRecordsFailedCounter.increment(currentBuffer.getEventCount());
        SdkBytes payload = currentBuffer.getPayload();
        if (dlqPushHandler != null) {
            dlqPushHandler.perform(pluginSetting, new LambdaSinkFailedDlqData(payload, e.getMessage(), 0));
            releaseEventHandles(true);
        } else {
            releaseEventHandles(false);
        }
    }

    private void handleLambdaResponse(InvokeResponse response) {
        int statusCode = response.statusCode();
        if (statusCode < 200 || statusCode >= 300) {
            LOG.warn("Lambda invocation returned with non-success status code: {}", statusCode);
        }
    }
}
