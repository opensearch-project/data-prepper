/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.sink;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.AbstractSink;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.lambda.common.LambdaCommonHandler;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.client.LambdaClientFactory;
import org.opensearch.dataprepper.plugins.lambda.common.config.BatchOptions;
import org.opensearch.dataprepper.plugins.lambda.sink.dlq.DlqPushHandler;
import org.opensearch.dataprepper.plugins.lambda.sink.dlq.LambdaSinkFailedDlqData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.opensearch.dataprepper.plugins.lambda.common.LambdaCommonHandler.createBufferBatches;

@DataPrepperPlugin(name = "aws_lambda", pluginType = Sink.class, pluginConfigurationType = LambdaSinkConfig.class)
public class LambdaSink extends AbstractSink<Record<Event>> {

    public static final String NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_SUCCESS = "lambdaProcessorObjectsEventsSucceeded";
    public static final String NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_FAILED = "lambdaProcessorObjectsEventsFailed";
    public static final String LAMBDA_LATENCY_METRIC = "lambdaProcessorLatency";
    public static final String REQUEST_PAYLOAD_SIZE = "requestPayloadSize";
    public static final String RESPONSE_PAYLOAD_SIZE = "responsePayloadSize";

    private static final Logger LOG = LoggerFactory.getLogger(LambdaSink.class);
    private static final String BUCKET = "bucket";
    private static final String KEY_PATH = "key_path_prefix";
    private final Counter numberOfRecordsSuccessCounter;
    private final Counter numberOfRecordsFailedCounter;
    private final LambdaSinkConfig lambdaSinkConfig;
    private final ExpressionEvaluator expressionEvaluator;
    private final LambdaAsyncClient lambdaAsyncClient;
    private final AtomicLong responsePayloadMetric;
    private final Timer lambdaLatencyMetric;
    private final AtomicLong requestPayloadMetric;
    private final PluginSetting pluginSetting;
    private volatile boolean sinkInitialized;
    private DlqPushHandler dlqPushHandler = null;

    @DataPrepperPluginConstructor
    public LambdaSink(final PluginSetting pluginSetting,
                      final LambdaSinkConfig lambdaSinkConfig,
                      final PluginFactory pluginFactory,
                      final SinkContext sinkContext,
                      final AwsCredentialsSupplier awsCredentialsSupplier,
                      final ExpressionEvaluator expressionEvaluator
    ) {
        super(pluginSetting);
        this.pluginSetting = pluginSetting;
        sinkInitialized = Boolean.FALSE;
        this.lambdaSinkConfig = lambdaSinkConfig;
        this.expressionEvaluator = expressionEvaluator;
        OutputCodecContext outputCodecContext = OutputCodecContext.fromSinkContext(sinkContext);
        this.responsePayloadMetric = pluginMetrics.gauge(RESPONSE_PAYLOAD_SIZE, new AtomicLong());
        this.numberOfRecordsSuccessCounter = pluginMetrics.counter(NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_SUCCESS);
        this.numberOfRecordsFailedCounter = pluginMetrics.counter(NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_FAILED);
        this.lambdaLatencyMetric = pluginMetrics.timer(LAMBDA_LATENCY_METRIC);
        this.requestPayloadMetric = pluginMetrics.gauge(REQUEST_PAYLOAD_SIZE, new AtomicLong());
        this.lambdaAsyncClient = LambdaClientFactory.createAsyncLambdaClient(
                lambdaSinkConfig.getAwsAuthenticationOptions(),
                lambdaSinkConfig.getMaxConnectionRetries(),
                awsCredentialsSupplier,
                lambdaSinkConfig.getConnectionTimeout()
        );
        if (lambdaSinkConfig.getDlqPluginSetting() != null) {
            this.dlqPushHandler = new DlqPushHandler(pluginFactory,
                    String.valueOf(lambdaSinkConfig.getDlqPluginSetting().get(BUCKET)),
                    lambdaSinkConfig.getDlqStsRoleARN()
                    , lambdaSinkConfig.getDlqStsRegion(),
                    String.valueOf(lambdaSinkConfig.getDlqPluginSetting().get(KEY_PATH)));
        }

    }

    @Override
    public boolean isReady() {
        return sinkInitialized;
    }

    @Override
    public void doInitialize() {
        try {
            doInitializeInternal();
        } catch (InvalidPluginConfigurationException e) {
            LOG.error("Invalid plugin configuration, Hence failed to initialize s3-sink plugin.");
            this.shutdown();
            throw e;
        } catch (Exception e) {
            LOG.error("Failed to initialize lambda plugin.");
            this.shutdown();
            throw e;
        }
    }

    private void doInitializeInternal() {
        sinkInitialized = Boolean.TRUE;
    }

    /**
     * @param records Records to be output
     */
    @Override
    public void doOutput(final Collection<Record<Event>> records) {

        if (records.isEmpty()) {
            return;
        }

        BatchOptions batchOptions = lambdaSinkConfig.getBatchOptions();
        String keyName = batchOptions.getKeyName();
        String whenCondition = lambdaSinkConfig.getWhenCondition();
        int maxEvents = batchOptions.getThresholdOptions().getEventCount();
        ByteCount maxBytes = batchOptions.getThresholdOptions().getMaximumSize();
        Duration maxCollectionDuration = batchOptions.getThresholdOptions().getEventCollectTimeOut();
        String functionName = lambdaSinkConfig.getFunctionName();
        String invocationType = lambdaSinkConfig.getInvocationType().getAwsLambdaValue();

        // Initialize here to void multi-threading issues
        // Note: By default, one instance of processor is created across threads.
        List<CompletableFuture<InvokeResponse>> futureList = new ArrayList<>();
        List<Record<Event>> resultRecords = new ArrayList<>();
        List<Buffer> batchedBuffers = createBufferBatches(records, keyName,
                whenCondition, expressionEvaluator, maxEvents, maxBytes, maxCollectionDuration, resultRecords);


        LOG.info("Batch Chunks created after threshold check: {}", batchedBuffers.size());
        for (Buffer buffer : batchedBuffers) {
            InvokeRequest requestPayload = buffer.getRequestPayload(functionName, invocationType);
            CompletableFuture<InvokeResponse> future = lambdaAsyncClient.invoke(requestPayload);
            futureList.add(future);
            future.thenAccept(response -> {
                handleLambdaResponse(buffer, response);
            }).exceptionally(throwable -> {
                handleFailure(throwable, buffer);
                return null;
            });
        }

        // Wait for all futures to complete
        LambdaCommonHandler.waitForFutures(futureList);

        // Release event handles for records not sent to Lambda
        for (Record<Event> record : records) {
            Event event = record.getData();
            releaseEventHandle(event, true);
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
        } catch (Exception ex) {
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
            int eventCount = flushedBuffer.getEventCount();
            LOG.info("Successfully flushed {} events", eventCount);
            SdkBytes payload = response.payload();
            if (payload == null || payload.asByteArray() == null || payload.asByteArray().length == 0) {
                responsePayloadMetric.set(0);
            } else {
                responsePayloadMetric.set(payload.asByteArray().length);
            }
            //metrics
            requestPayloadMetric.set(flushedBuffer.getPayloadRequestSize());
            numberOfRecordsSuccessCounter.increment(eventCount);
            Duration latency = flushedBuffer.stopLatencyWatch();
            lambdaLatencyMetric.record(latency.toMillis(), TimeUnit.MILLISECONDS);
        } else {
            // Non-2xx status code treated as failure
            handleFailure(new RuntimeException("Non-success Lambda status code: " + response.statusCode()), flushedBuffer);
        }
    }
}