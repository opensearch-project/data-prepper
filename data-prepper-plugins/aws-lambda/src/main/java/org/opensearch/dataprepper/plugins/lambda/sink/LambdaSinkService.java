/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.sink;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
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
import org.opensearch.dataprepper.plugins.lambda.common.util.ThresholdCheck;
import org.opensearch.dataprepper.plugins.lambda.sink.dlq.DlqPushHandler;
import org.opensearch.dataprepper.plugins.lambda.sink.dlq.LambdaSinkFailedDlqData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.lambda.LambdaClient;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LambdaSinkService {

    private static final Logger LOG = LoggerFactory.getLogger(LambdaSinkService.class);
    public static final String NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_SUCCESS = "lambdaSinkObjectsEventsSucceeded";
    public static final String NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_FAILED = "lambdaSinkObjectsEventsFailed";
    private final PluginSetting pluginSetting;
    private final Lock reentrantLock;
    private final LambdaSinkConfig lambdaSinkConfig;
    private final LambdaClient lambdaClient;
    private final String functionName;
    private int maxEvents = 0;
    private ByteCount maxBytes = null;
    private Duration maxCollectionDuration = null;
    private int maxRetries = 0;
    private final Counter numberOfRecordsSuccessCounter;
    private final Counter numberOfRecordsFailedCounter;
    private final String ASYNC_INVOCATION_TYPE = "Event";
    private final String invocationType;
    private Buffer currentBuffer;
    private final BufferFactory bufferFactory;
    private final DlqPushHandler dlqPushHandler;
    private final Collection<EventHandle> bufferedEventHandles;
    private final List<Event> events;
    private OutputCodec codec = null;
    private final BatchOptions batchOptions;
    private final Boolean isBatchEnabled;
    private OutputCodecContext codecContext = null;
    private String payloadModel = null;

    public LambdaSinkService(final LambdaClient lambdaClient,
                             final LambdaSinkConfig lambdaSinkConfig,
                             final PluginMetrics pluginMetrics,
                             final PluginFactory pluginFactory,
                             final PluginSetting pluginSetting,
                             final OutputCodecContext codecContext,
                             final AwsCredentialsSupplier awsCredentialsSupplier,
                             final DlqPushHandler dlqPushHandler,
                             final BufferFactory bufferFactory) {
        this.lambdaSinkConfig = lambdaSinkConfig;
        this.pluginSetting = pluginSetting;
        this.dlqPushHandler = dlqPushHandler;
        this.lambdaClient = lambdaClient;
        reentrantLock = new ReentrantLock();
        numberOfRecordsSuccessCounter = pluginMetrics.counter(NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_SUCCESS);
        numberOfRecordsFailedCounter = pluginMetrics.counter(NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_FAILED);
        functionName = lambdaSinkConfig.getFunctionName();
        payloadModel = lambdaSinkConfig.getPayloadModel();
        maxRetries = lambdaSinkConfig.getMaxConnectionRetries();
        batchOptions = lambdaSinkConfig.getBatchOptions();

        if (payloadModel.equals(BATCH_EVENT)){
            JsonOutputCodecConfig jsonOutputCodecConfig = new JsonOutputCodecConfig();
            jsonOutputCodecConfig.setKeyName(batchOptions.getKeyName());
            codec = new JsonOutputCodec(jsonOutputCodecConfig);
            maxEvents = batchOptions.getThresholdOptions().getEventCount();
            maxBytes = batchOptions.getThresholdOptions().getMaximumSize();
            maxCollectionDuration = batchOptions.getThresholdOptions().getEventCollectTimeOut();
            isBatchEnabled = true;
        } else if(payloadModel.equals(SINGLE_EVENT)){
            NdjsonOutputConfig ndjsonOutputCodecConfig = new NdjsonOutputConfig();
            codec = new NdjsonOutputCodec(ndjsonOutputCodecConfig);
            isBatchEnabled = false;
        } else{
            throw new RuntimeException("invalid payload_model option");
        }
        this.codecContext = codecContext;
        bufferedEventHandles = new LinkedList<>();
        events = new ArrayList();

        invocationType = ASYNC_INVOCATION_TYPE;

        this.bufferFactory = bufferFactory;
        try {
            currentBuffer = bufferFactory.getBuffer(lambdaClient,functionName,invocationType);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void output(Collection<Record<Event>> records){
        // Don't acquire the lock if there's no work to be done
        if (records.isEmpty() && currentBuffer.getEventCount() == 0) {
            return;
        }
        List<Event> failedEvents = new ArrayList<>();
        Exception sampleException = null;
        reentrantLock.lock();
        try {
            for (Record<Event> record : records) {
                final Event event = record.getData();
                try {
                    if (currentBuffer.getEventCount() == 0) {
                        codec.start(currentBuffer.getOutputStream(), event, codecContext);
                    }
                    codec.writeEvent(event, currentBuffer.getOutputStream());
                    int count = currentBuffer.getEventCount() + 1;
                    currentBuffer.setEventCount(count);

                    bufferedEventHandles.add(event.getEventHandle());
                } catch (Exception ex) {
                    if(sampleException == null) {
                        sampleException = ex;
                    }
                    failedEvents.add(event);
                }

                flushToLambdaIfNeeded();
            }
        } finally {
            reentrantLock.unlock();
        }

        if(!failedEvents.isEmpty()) {
            failedEvents
                    .stream()
                    .map(Event::getEventHandle)
                    .forEach(eventHandle -> eventHandle.release(false));
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
        LOG.trace("Flush to Lambda check: currentBuffer.size={}, currentBuffer.events={}, currentBuffer.duration={}",
                currentBuffer.getSize(), currentBuffer.getEventCount(), currentBuffer.getDuration());
        final AtomicReference<String> errorMsgObj = new AtomicReference<>();

        try {
            if (ThresholdCheck.checkThresholdExceed(currentBuffer, maxEvents, maxBytes, maxCollectionDuration, isBatchEnabled)) {
                try {
                    codec.complete(currentBuffer.getOutputStream());
                    LOG.info("Writing {} to Lambda with {} events and size of {} bytes.",
                            functionName, currentBuffer.getEventCount(), currentBuffer.getSize());
                    final boolean isFlushToLambda = retryFlushToLambda(currentBuffer, errorMsgObj);

                    if (isFlushToLambda) {
                        LOG.info("Successfully flushed to Lambda {}.", functionName);
                        numberOfRecordsSuccessCounter.increment(currentBuffer.getEventCount());
                        releaseEventHandles(true);
                    } else {
                        LOG.error("Failed to save to Lambda {}", functionName);
                        numberOfRecordsFailedCounter.increment(currentBuffer.getEventCount());
                        SdkBytes payload = currentBuffer.getPayload();
                        if(dlqPushHandler!=null) {
                            dlqPushHandler.perform(pluginSetting, new LambdaSinkFailedDlqData(payload, errorMsgObj.get(), 0));
                            releaseEventHandles(true);
                        }else{
                            releaseEventHandles(false);
                        }
                    }

                    //reset buffer after flush
                    currentBuffer = bufferFactory.getBuffer(lambdaClient,functionName,invocationType);
                } catch (final IOException e) {
                    releaseEventHandles(false);
                    LOG.error("Exception while completing codec", e);
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    protected boolean retryFlushToLambda(Buffer currentBuffer,
                                                    final AtomicReference<String> errorMsgObj) throws InterruptedException {
        boolean isUploadedToLambda = Boolean.FALSE;
        int retryCount = maxRetries;
        do {

            try {
                currentBuffer.flushToLambdaAsync();
                isUploadedToLambda = Boolean.TRUE;
            } catch (AwsServiceException | SdkClientException e) {
                errorMsgObj.set(e.getMessage());
                LOG.error("Exception occurred while uploading records to lambda. Retry countdown  : {} | exception:",
                        retryCount, e);
                --retryCount;
                if (retryCount == 0) {
                    return isUploadedToLambda;
                }
                Thread.sleep(5000);
            }
        } while (!isUploadedToLambda);
        return isUploadedToLambda;
    }
}
