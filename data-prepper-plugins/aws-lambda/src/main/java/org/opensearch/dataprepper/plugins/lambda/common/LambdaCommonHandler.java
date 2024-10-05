package org.opensearch.dataprepper.plugins.lambda.common;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.BufferFactory;
import org.opensearch.dataprepper.plugins.lambda.common.util.ThresholdCheck;
import org.opensearch.dataprepper.plugins.lambda.sink.dlq.DlqPushHandler;
import org.opensearch.dataprepper.plugins.lambda.sink.dlq.LambdaSinkFailedDlqData;
import org.slf4j.Logger;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class LambdaCommonHandler {
    private final Logger LOG;
    private final LambdaAsyncClient lambdaAsyncClient;
    private final String functionName;
    private final String invocationType;
    private final ExpressionEvaluator expressionEvaluator;
    private final Counter numberOfRecordsSuccessCounter;
    private final Counter numberOfRecordsFailedCounter;
    private final Timer lambdaLatencyMetric;
    private final BufferFactory bufferFactory;
    private final Boolean isBatchEnabled;
    private Buffer currentBuffer;
    private final OutputCodec codec;
    private final OutputCodecContext codecContext;
    final List<CompletableFuture<Void>> futureList;
    private final Collection<EventHandle> bufferedEventHandles;
    List<Record<Event>> resultRecords;
    private final String whenCondition;
    private final int maxEvents;
    private final ByteCount maxBytes;
    private final Duration maxCollectionDuration;
    private final Boolean isSink;
    private final DlqPushHandler dlqPushHandler;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final PluginSetting pluginSetting;


    public LambdaCommonHandler(
            final Logger log,
            final LambdaAsyncClient lambdaAsyncClient,
            final String functionName,
            final String invocationType,
            final ExpressionEvaluator expressionEvaluator,
            final Counter successCounter,
            final Counter failureCounter,
            final Timer latencyMetric,
            final BufferFactory bufferFactory,
            final OutputCodec codec,
            final OutputCodecContext codecContext,
            final Boolean isBatchEnabled,
            final String whenCondition,
            final int maxEvents,
            final ByteCount maxBytes,
            final Duration maxCollectionDuration,
            final Boolean isSink,
            DlqPushHandler dlqPushHandler,
            final PluginSetting pluginSetting) {

        this.LOG = log;
        this.lambdaAsyncClient = lambdaAsyncClient;
        this.functionName = functionName;
        this.invocationType = invocationType;
        this.expressionEvaluator = expressionEvaluator;
        this.numberOfRecordsSuccessCounter = successCounter;
        this.numberOfRecordsFailedCounter = failureCounter;
        this.lambdaLatencyMetric = latencyMetric;
        this.bufferFactory = bufferFactory;
        this.codec = codec;
        this.codecContext = codecContext;
        this.isBatchEnabled = isBatchEnabled;
        this.whenCondition = whenCondition;
        this.maxEvents = maxEvents;
        this.maxBytes = maxBytes;
        this.maxCollectionDuration = maxCollectionDuration;
        this.isSink = isSink;
        this.dlqPushHandler = dlqPushHandler;
        this.pluginSetting = pluginSetting;

        this.futureList = new ArrayList<>();
        this.bufferedEventHandles = new LinkedList<>();

        try {
            currentBuffer = bufferFactory.getBuffer(lambdaAsyncClient, functionName, invocationType);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void processEvent(List<Record<Event>> resultRecords, final Event event) throws Exception {
        if (currentBuffer.getEventCount() == 0) {
            codec.start(currentBuffer.getOutputStream(), event, codecContext);
        }
        codec.writeEvent(event, currentBuffer.getOutputStream());
        int count = currentBuffer.getEventCount() + 1;
        LOG.info("CurrentBuffer event count: {}", count);
        currentBuffer.setEventCount(count);

        if(isSink) bufferedEventHandles.add(event.getEventHandle());

        flushToLambdaIfNeeded(resultRecords, false);
    }

    public void flushToLambdaIfNeeded(List<Record<Event>> resultRecords, boolean forceFlush) {

        LOG.info("currentBufferEventCount:{}, maxEvents:{}, maxBytes:{}, maxCollectionDuration:{}, isBatch:{}, forceFlush:{} ", currentBuffer.getEventCount(),maxEvents,maxBytes,maxCollectionDuration,isBatchEnabled, forceFlush);
        if (forceFlush || ThresholdCheck.checkThresholdExceed(currentBuffer, maxEvents, maxBytes, maxCollectionDuration, isBatchEnabled)) {
            try {
                codec.complete(currentBuffer.getOutputStream());

                // Capture buffer before resetting
                Buffer flushedBuffer = currentBuffer;
                int eventCount = currentBuffer.getEventCount();
                LOG.info("Event Count before calling lambda:{}",eventCount);
                CompletableFuture<InvokeResponse> future = flushedBuffer.flushToLambdaAsync(invocationType);

                // Handle future
                CompletableFuture<Void> processingFuture = future.thenAccept(response -> {
                    handleLambdaResponse(response);
                    LOG.info("Successfully flushed {} events", eventCount);
                    numberOfRecordsSuccessCounter.increment(eventCount);
                    lambdaLatencyMetric.record(flushedBuffer.getFlushLambdaSyncLatencyMetric());
                    if(!isSink){
                        synchronized (resultRecords) {
                            Event lambdaEvent = convertLambdaResponseToEvent(response);
                            resultRecords.add(new Record<>(lambdaEvent));
                        }
                    }
                    if(isSink) releaseEventHandles(true);

                }).exceptionally(throwable -> {
                    LOG.error("Exception occurred while invoking Lambda. Function: {} | Exception: ", functionName, throwable);
                    handleFailure(throwable);
                    if(isSink)  releaseEventHandles(false);
                    return null;
                });

                futureList.add(processingFuture);
                resetBuffer();
            } catch (IOException e) {
                LOG.error("Exception while flushing to lambda", e);
                handleFailure(e);
                if(isSink) releaseEventHandles(false);
                resetBuffer();
            }
        }
    }

    public Buffer getCurrentBuffer(){
        return currentBuffer;
    }

    public void resetBuffer() {
        try {
            LOG.info("Resetting buffer");
            currentBuffer = bufferFactory.getBuffer(lambdaAsyncClient, functionName, invocationType);
        } catch (IOException e) {
            throw new RuntimeException("Failed to reset buffer", e);
        }
    }

    public void handleLambdaResponse(InvokeResponse response) {
        int statusCode = response.statusCode();
        if (statusCode < 200 || statusCode >= 300) {
            LOG.warn("Lambda invocation returned with non-success status code: {}", statusCode);
        }
    }

    public void releaseEventHandles(final boolean result) {
        for (EventHandle eventHandle : bufferedEventHandles) {
            eventHandle.release(result);
        }
        bufferedEventHandles.clear();
    }

    public void waitForFutures() {
        if (!futureList.isEmpty()) {
            try {
                CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0])).join();
                LOG.info("All {} Lambda invocations have completed", futureList.size());
            } catch (Exception e) {
                LOG.warn("Exception while waiting for Lambda invocations to complete", e);
            } finally {
                futureList.clear();
            }
        }
    }


    public void handleFailure(Throwable throwable) {
        numberOfRecordsFailedCounter.increment(currentBuffer.getEventCount());
        if(isSink) {
            SdkBytes payload = currentBuffer.getPayload();
            if (dlqPushHandler != null) {
                dlqPushHandler.perform(pluginSetting, new LambdaSinkFailedDlqData(payload, throwable.getMessage(), 0));
                releaseEventHandles(true);
            } else {
                releaseEventHandles(false);
            }
        }
    }

    public Event convertLambdaResponseToEvent(InvokeResponse lambdaResponse) {
        try {
            SdkBytes payload = lambdaResponse.payload();
            if (payload != null) {
                String payloadJsonString = payload.asString(StandardCharsets.UTF_8);

                JsonNode jsonNode = null;
                try {
                    jsonNode = objectMapper.readTree(payloadJsonString);
                } catch (JsonParseException e) {
                    throw new RuntimeException("payload output is not json formatted");
                }
                return JacksonEvent.builder().withEventType("event").withData(jsonNode).build();
            }
        } catch (Exception e) {
            LOG.error("Error converting Lambda response to Event", e);
            throw new RuntimeException("Error converting Lambda response to Event");
        }
        return null;
    }
}
