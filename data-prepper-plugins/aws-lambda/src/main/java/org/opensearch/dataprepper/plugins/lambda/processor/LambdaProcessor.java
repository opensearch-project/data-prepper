/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.processor;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.annotations.SingleThread;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodec;
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodecConfig;
import org.opensearch.dataprepper.plugins.lambda.common.LambdaCommonHandler;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.BufferFactory;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.InMemoryBufferFactory;
import org.opensearch.dataprepper.plugins.lambda.common.client.LambdaClientFactory;
import org.opensearch.dataprepper.plugins.lambda.common.config.BatchOptions;
import org.opensearch.dataprepper.plugins.lambda.common.util.ThresholdCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@DataPrepperPlugin(name = "aws_lambda", pluginType = Processor.class, pluginConfigurationType = LambdaProcessorConfig.class)
public class LambdaProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {

    public static final String NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_SUCCESS = "lambdaProcessorObjectsEventsSucceeded";
    public static final String NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_FAILED = "lambdaProcessorObjectsEventsFailed";
    public static final String LAMBDA_LATENCY_METRIC = "lambdaProcessorLatency";
    public static final String REQUEST_PAYLOAD_SIZE = "requestPayloadSize";
    public static final String RESPONSE_PAYLOAD_SIZE = "responsePayloadSize";

    private static final Logger LOG = LoggerFactory.getLogger(LambdaProcessor.class);

    private final String functionName;
    private final String whenCondition;
    private final ExpressionEvaluator expressionEvaluator;
    private final Counter numberOfRecordsSuccessCounter;
    private final Counter numberOfRecordsFailedCounter;
    private final Timer lambdaLatencyMetric;
    private final String invocationType;
    private final List<String> tagsOnMatchFailure;
    private final BatchOptions batchOptions;
    private final LambdaAsyncClient lambdaAsyncClient;
    private final AtomicLong requestPayloadMetric;
    private final AtomicLong responsePayloadMetric;
    OutputCodecContext codecContext = null;
    LambdaCommonHandler lambdaCommonHandler;
    private int maxEvents = 0;
    private ByteCount maxBytes = null;
    private Duration maxCollectionDuration = null;
    private int maxRetries = 0;
    private int totalFlushedEvents;
    PluginSetting codecPluginSetting;
    PluginFactory pluginFactory;
    private final ResponseEventHandlingStrategy responseStrategy;

    @SingleThread
    @DataPrepperPluginConstructor
    public LambdaProcessor(final PluginFactory pluginFactory, final PluginMetrics pluginMetrics, final LambdaProcessorConfig lambdaProcessorConfig, final AwsCredentialsSupplier awsCredentialsSupplier, final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.expressionEvaluator = expressionEvaluator;
        this.pluginFactory = pluginFactory;
        this.numberOfRecordsSuccessCounter = pluginMetrics.counter(NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_SUCCESS);
        this.numberOfRecordsFailedCounter = pluginMetrics.counter(NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_FAILED);
        this.lambdaLatencyMetric = pluginMetrics.timer(LAMBDA_LATENCY_METRIC);
        this.requestPayloadMetric = pluginMetrics.gauge(REQUEST_PAYLOAD_SIZE, new AtomicLong());
        this.responsePayloadMetric = pluginMetrics.gauge(RESPONSE_PAYLOAD_SIZE, new AtomicLong());

        functionName = lambdaProcessorConfig.getFunctionName();
        whenCondition = lambdaProcessorConfig.getWhenCondition();
        maxRetries = lambdaProcessorConfig.getMaxConnectionRetries();
        batchOptions = lambdaProcessorConfig.getBatchOptions();
        tagsOnMatchFailure = lambdaProcessorConfig.getTagsOnMatchFailure();
        codecContext = new OutputCodecContext();
        PluginModel responseCodecConfig = lambdaProcessorConfig.getResponseCodecConfig();

        if (responseCodecConfig == null) {
            // Default to JsonInputCodec with default settings
            codecPluginSetting = new PluginSetting("json", Collections.emptyMap());
        } else {
            codecPluginSetting = new PluginSetting(responseCodecConfig.getPluginName(), responseCodecConfig.getPluginSettings());
        }

        maxEvents = batchOptions.getThresholdOptions().getEventCount();
        maxBytes = batchOptions.getThresholdOptions().getMaximumSize();
        maxCollectionDuration = batchOptions.getThresholdOptions().getEventCollectTimeOut();
        invocationType = lambdaProcessorConfig.getInvocationType().getAwsLambdaValue();

        lambdaAsyncClient = LambdaClientFactory.createAsyncLambdaClient(lambdaProcessorConfig.getAwsAuthenticationOptions(),
            lambdaProcessorConfig.getMaxConnectionRetries(), awsCredentialsSupplier, lambdaProcessorConfig.getConnectionTimeout());

        // Select the correct strategy based on the configuration
        if (lambdaProcessorConfig.getResponseEventsMatch()) {
            this.responseStrategy = new StrictResponseEventHandlingStrategy();
        } else {
            this.responseStrategy = new AggregateResponseEventHandlingStrategy();
        }

        LOG.info("LambdaFunctionName:{} , responseEventsMatch:{}, invocationType:{}", functionName,
                lambdaProcessorConfig.getResponseEventsMatch(), invocationType);
    }

    @Override
    public Collection<Record<Event>> doExecute(Collection<Record<Event>> records) {
        if (records.isEmpty()) {
            return records;
        }

        // Initialize here to void multi-threading issues
        // Note: By default, one instance of processor is created across threads.
        BufferFactory bufferFactory = new InMemoryBufferFactory();
        lambdaCommonHandler = new LambdaCommonHandler(LOG, lambdaAsyncClient, functionName, invocationType);
        Buffer currentBufferPerBatch = lambdaCommonHandler.createBuffer(bufferFactory);
        List futureList = new ArrayList<>();
        totalFlushedEvents = 0;

        // Setup request codec
        JsonOutputCodecConfig jsonOutputCodecConfig = new JsonOutputCodecConfig();
        jsonOutputCodecConfig.setKeyName(batchOptions.getKeyName());
        OutputCodec requestCodec = new JsonOutputCodec(jsonOutputCodecConfig);

        //Setup response codec
        InputCodec responseCodec = pluginFactory.loadPlugin(InputCodec.class, codecPluginSetting);

        List<Record<Event>> resultRecords = new ArrayList<>();

        LOG.info("Batch size received to lambda processor: {}", records.size());
        for (Record<Event> record : records) {
            final Event event = record.getData();

            // If the condition is false, add the event to resultRecords as-is
            if (whenCondition != null && !expressionEvaluator.evaluateConditional(whenCondition, event)) {
                resultRecords.add(record);
                continue;
            }

            try {
                if (currentBufferPerBatch.getEventCount() == 0) {
                    requestCodec.start(currentBufferPerBatch.getOutputStream(), event, new OutputCodecContext());
                }
                requestCodec.writeEvent(event, currentBufferPerBatch.getOutputStream());
                currentBufferPerBatch.addRecord(record);

                boolean wasFlushed =  flushToLambdaIfNeeded(resultRecords, currentBufferPerBatch,
                        requestCodec, responseCodec,futureList,false);

                // After flushing, create a new buffer for the next batch
                if (wasFlushed) {
                    currentBufferPerBatch = lambdaCommonHandler.createBuffer(bufferFactory);
                    requestCodec = new JsonOutputCodec(jsonOutputCodecConfig);
                }
            } catch (Exception e) {
                LOG.error(NOISY, "Exception while processing event {}", event, e);
                handleFailure(e, currentBufferPerBatch, resultRecords);
                currentBufferPerBatch = lambdaCommonHandler.createBuffer(bufferFactory);
                requestCodec = new JsonOutputCodec(jsonOutputCodecConfig);
            }
        }

        // Flush any remaining events in the buffer after processing all records
        if (currentBufferPerBatch.getEventCount() > 0) {
            LOG.info("Force Flushing the remaining {} events in the buffer", currentBufferPerBatch.getEventCount());
            try {
                flushToLambdaIfNeeded(resultRecords, currentBufferPerBatch,
                        requestCodec, responseCodec, futureList,true);
                currentBufferPerBatch.reset();
            } catch (Exception e) {
                LOG.error("Exception while flushing remaining events", e);
                handleFailure(e, currentBufferPerBatch, resultRecords);
            }
        }

        lambdaCommonHandler.waitForFutures(futureList);
        LOG.info("Total events flushed to lambda successfully: {}", totalFlushedEvents);

        return resultRecords;
    }


    boolean flushToLambdaIfNeeded(List<Record<Event>> resultRecords, Buffer currentBufferPerBatch,
                               OutputCodec requestCodec, InputCodec responseCodec, List futureList,
                               boolean forceFlush) {

        LOG.debug("currentBufferPerBatchEventCount:{}, maxEvents:{}, maxBytes:{}, " +
                "maxCollectionDuration:{}, forceFlush:{} ", currentBufferPerBatch.getEventCount(),
                maxEvents, maxBytes, maxCollectionDuration, forceFlush);
        if (forceFlush || ThresholdCheck.checkThresholdExceed(currentBufferPerBatch, maxEvents, maxBytes, maxCollectionDuration)) {
            try {
                requestCodec.complete(currentBufferPerBatch.getOutputStream());

                // Capture buffer before resetting
                final int eventCount = currentBufferPerBatch.getEventCount();

                CompletableFuture<InvokeResponse> future = currentBufferPerBatch.flushToLambda(invocationType);

                // Handle future
                CompletableFuture<Void> processingFuture = future.thenAccept(response -> {
                    //Success handler
                    handleLambdaResponse(resultRecords, currentBufferPerBatch, eventCount, response, responseCodec);
                }).exceptionally(throwable -> {
                    //Failure handler
                    List<Record<Event>> bufferRecords = currentBufferPerBatch.getRecords();
                    Record<Event> eventRecord = bufferRecords.isEmpty() ? null : bufferRecords.get(0);
                    LOG.error(NOISY, "Exception occurred while invoking Lambda. Function: {} , Event: {}",
                                functionName, eventRecord == null? "null":eventRecord.getData(), throwable);
                    requestPayloadMetric.set(currentBufferPerBatch.getPayloadRequestSize());
                    responsePayloadMetric.set(0);
                    Duration latency = currentBufferPerBatch.stopLatencyWatch();
                    lambdaLatencyMetric.record(latency.toMillis(), TimeUnit.MILLISECONDS);
                    handleFailure(throwable, currentBufferPerBatch, resultRecords);
                    return null;
                });

                futureList.add(processingFuture);
            } catch (IOException e) {
                LOG.error(NOISY, "Exception while flushing to lambda", e);
                handleFailure(e, currentBufferPerBatch, resultRecords);
            } finally {
                return true;
            }
        }
        return false;
    }

    private void handleLambdaResponse(List<Record<Event>> resultRecords, Buffer flushedBuffer,
                                      int eventCount, InvokeResponse response, InputCodec responseCodec) {
        boolean success = lambdaCommonHandler.checkStatusCode(response);
        if (success) {
            LOG.info("Successfully flushed {} events", eventCount);

            //metrics
            numberOfRecordsSuccessCounter.increment(eventCount);
            Duration latency = flushedBuffer.stopLatencyWatch();
            lambdaLatencyMetric.record(latency.toMillis(), TimeUnit.MILLISECONDS);
            totalFlushedEvents += eventCount;

            convertLambdaResponseToEvent(resultRecords, response, flushedBuffer, responseCodec);
        } else {
            // Non-2xx status code treated as failure
            handleFailure(new RuntimeException("Non-success Lambda status code: " + response.statusCode()), flushedBuffer, resultRecords);
        }
    }

    /*
     * Assumption: Lambda always returns json array.
     * 1. If response has an array, we assume that we split the individual events.
     * 2. If it is not an array, then create one event per response.
     */
    void convertLambdaResponseToEvent(final List<Record<Event>> resultRecords, final InvokeResponse lambdaResponse,
                                      Buffer flushedBuffer, InputCodec responseCodec) {
        try {
            List<Event> parsedEvents = new ArrayList<>();
            List<Record<Event>> originalRecords = flushedBuffer.getRecords();

            SdkBytes payload = lambdaResponse.payload();
            // Handle null or empty payload
            if (payload == null || payload.asByteArray() == null || payload.asByteArray().length == 0) {
                LOG.warn(NOISY, "Lambda response payload is null or empty, dropping the original events");
                // Set metrics
                requestPayloadMetric.set(flushedBuffer.getPayloadRequestSize());
                responsePayloadMetric.set(0);
            } else {
                // Set metrics
                requestPayloadMetric.set(flushedBuffer.getPayloadRequestSize());
                responsePayloadMetric.set(payload.asByteArray().length);

                LOG.debug("Response payload:{}", payload.asUtf8String());
                InputStream inputStream = new ByteArrayInputStream(payload.asByteArray());
                //Convert to response codec
                try {
                    responseCodec.parse(inputStream, record -> {
                        Event event = record.getData();
                        parsedEvents.add(event);
                    });
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }

                LOG.debug("Parsed Event Size:{}, FlushedBuffer eventCount:{}, " +
                        "FlushedBuffer size:{}", parsedEvents.size(), flushedBuffer.getEventCount(),
                        flushedBuffer.getSize());
                responseStrategy.handleEvents(parsedEvents, originalRecords, resultRecords, flushedBuffer);
            }


        } catch (Exception e) {
            LOG.error(NOISY, "Error converting Lambda response to Event");
            // Metrics update
            requestPayloadMetric.set(flushedBuffer.getPayloadRequestSize());
            responsePayloadMetric.set(0);
            handleFailure(e, flushedBuffer, resultRecords);
        }
    }

    /*
     * If one event in the Buffer fails, we consider that the entire
     * Batch fails and tag each event in that Batch.
     */
    void handleFailure(Throwable e, Buffer flushedBuffer, List<Record<Event>> resultRecords) {
        try {
            if (flushedBuffer.getEventCount() > 0) {
                numberOfRecordsFailedCounter.increment(flushedBuffer.getEventCount());
            }

            addFailureTags(flushedBuffer, resultRecords);
            LOG.error(NOISY, "Failed to process batch due to error: ", e);
        } catch(Exception ex){
            LOG.error(NOISY, "Exception in handleFailure while processing failure for buffer: ", ex);
        }
    }

    private void addFailureTags(Buffer flushedBuffer, List<Record<Event>> resultRecords) {
        // Add failure tags to each event in the batch
        for (Record<Event> record : flushedBuffer.getRecords()) {
            Event event = record.getData();
            EventMetadata metadata = event.getMetadata();
            if (metadata != null) {
                metadata.addTags(tagsOnMatchFailure);
            } else {
                LOG.warn("Event metadata is null, cannot add failure tags.");
            }
            resultRecords.add(record);
        }
    }


    @Override
    public void prepareForShutdown() {

    }

    @Override
    public boolean isReadyForShutdown() {
        return true;
    }

    @Override
    public void shutdown() {
    }

}