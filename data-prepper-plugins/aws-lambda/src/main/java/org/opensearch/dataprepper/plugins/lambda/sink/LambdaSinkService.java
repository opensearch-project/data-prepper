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
import org.opensearch.dataprepper.plugins.lambda.common.config.BatchOptions;
import org.opensearch.dataprepper.plugins.lambda.common.util.ThresholdCheck;
import org.opensearch.dataprepper.plugins.lambda.sink.dlq.DlqPushHandler;
import org.opensearch.dataprepper.plugins.lambda.sink.dlq.LambdaSinkFailedDlqData;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.InMemoryBufferFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
    private OutputCodec requestCodec = null;
    private OutputCodecContext codecContext = null;
    private LambdaCommonHandler lambdaCommonHandler;
    private Buffer currentBufferPerBatch = null;
    List<CompletableFuture<Void>> futureList;
    private final JsonOutputCodecConfig jsonOutputCodecConfig;



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

        jsonOutputCodecConfig = new JsonOutputCodecConfig();
        jsonOutputCodecConfig.setKeyName(batchOptions.getKeyName());

        maxEvents = batchOptions.getThresholdOptions().getEventCount();
        maxBytes = batchOptions.getThresholdOptions().getMaximumSize();
        maxCollectionDuration = batchOptions.getThresholdOptions().getEventCollectTimeOut();
        invocationType = lambdaSinkConfig.getInvocationType().getAwsLambdaValue();
        futureList = Collections.synchronizedList(new ArrayList<>());

        this.bufferFactory = bufferFactory;
    }


    public void output(Collection<Record<Event>> records) {
        if (records.isEmpty()) {
            return;
        }

        //Result from lambda is not currently processes.
        BufferFactory bufferFactory = new InMemoryBufferFactory();
        lambdaCommonHandler = new LambdaCommonHandler(LOG, lambdaAsyncClient, jsonOutputCodecConfig, lambdaSinkConfig);
        lambdaCommonHandler.sendRecords(records,
                (inputBuffer, resultRecords)->{
                    releaseEventHandlesPerBatch(true, inputBuffer);
                },
                (inputBuffer, resultRecords)->{
                    handleFailure(new RuntimeException("failed"), inputBuffer);
                });
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
            if (event != null) {
                EventHandle eventHandle = event.getEventHandle();
                if (eventHandle != null) {
                    eventHandle.release(success);
                }
            }
        }
    }

}
