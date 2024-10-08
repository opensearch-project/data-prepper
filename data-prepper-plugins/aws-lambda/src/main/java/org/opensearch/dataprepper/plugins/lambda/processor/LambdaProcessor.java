/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.EVENT;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodec;
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodecConfig;
import org.opensearch.dataprepper.plugins.codec.json.NdjsonOutputCodec;
import org.opensearch.dataprepper.plugins.codec.json.NdjsonOutputConfig;
import org.opensearch.dataprepper.plugins.lambda.common.LambdaCommonHandler;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.BufferFactory;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.InMemoryBufferFactory;
import org.opensearch.dataprepper.plugins.lambda.common.client.LambdaClientFactory;
import org.opensearch.dataprepper.plugins.lambda.common.config.BatchOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig;
import static org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig.BATCH_EVENT;
import static org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig.SINGLE_EVENT;
import static org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig.invocationTypeMap;
import org.opensearch.dataprepper.plugins.lambda.sink.dlq.DlqPushHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
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
    private final Collection<EventHandle> bufferedEventHandles;
    private final BatchOptions batchOptions;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final BufferFactory bufferFactory;
    private final LambdaAsyncClient lambdaAsyncClient;
    private final Boolean isBatchEnabled;
    private final AtomicLong requestPayloadMetric;
    private final AtomicLong responsePayload;
    private String payloadModel = null;
    private int maxEvents = 0;
    private ByteCount maxBytes = null;
    private Duration maxCollectionDuration = null;
    private int maxRetries = 0;
    private OutputCodec codec = null;
    OutputCodecContext codecContext = null;
    LambdaCommonHandler lambdaCommonHandler;

    @DataPrepperPluginConstructor
    public LambdaProcessor(final PluginMetrics pluginMetrics, final LambdaProcessorConfig lambdaProcessorConfig, final AwsCredentialsSupplier awsCredentialsSupplier, final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.expressionEvaluator = expressionEvaluator;
        this.numberOfRecordsSuccessCounter = pluginMetrics.counter(NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_SUCCESS);
        this.numberOfRecordsFailedCounter = pluginMetrics.counter(NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_FAILED);
        this.lambdaLatencyMetric = pluginMetrics.timer(LAMBDA_LATENCY_METRIC);
        this.requestPayloadMetric = pluginMetrics.gauge(REQUEST_PAYLOAD_SIZE, new AtomicLong());
        this.responsePayload = pluginMetrics.gauge(RESPONSE_PAYLOAD_SIZE, new AtomicLong());

        functionName = lambdaProcessorConfig.getFunctionName();
        whenCondition = lambdaProcessorConfig.getWhenCondition();
        maxRetries = lambdaProcessorConfig.getMaxConnectionRetries();
        batchOptions = lambdaProcessorConfig.getBatchOptions();
        payloadModel = lambdaProcessorConfig.getPayloadModel();
        codecContext = new OutputCodecContext();

        if (payloadModel.equals(BATCH_EVENT)) {
            JsonOutputCodecConfig jsonOutputCodecConfig = new JsonOutputCodecConfig();
            jsonOutputCodecConfig.setKeyName(batchOptions.getKeyName());
            codec = new JsonOutputCodec(jsonOutputCodecConfig);
            maxEvents = batchOptions.getThresholdOptions().getEventCount();
            maxBytes = batchOptions.getThresholdOptions().getMaximumSize(); // remove
            maxCollectionDuration = batchOptions.getThresholdOptions().getEventCollectTimeOut();
            isBatchEnabled = true;
            LOG.debug("maxEvents:" + maxEvents + " maxbytes:" + maxBytes + " maxDuration:" + maxCollectionDuration);
        } else if(payloadModel.equals(SINGLE_EVENT)) {
            NdjsonOutputConfig ndjsonOutputCodecConfig = new NdjsonOutputConfig();
            codec = new NdjsonOutputCodec(ndjsonOutputCodecConfig);
            isBatchEnabled = false;
            LOG.debug("isBatchEnabled:{}",isBatchEnabled);
        } else{
            throw new RuntimeException("invalid payload_model option");
        }

        //EVENT type will soon be supported.
        if(lambdaProcessorConfig.getInvocationType().equals(LambdaCommonConfig.EVENT) &&
                !lambdaProcessorConfig.getInvocationType().equals(LambdaCommonConfig.REQUEST_RESPONSE)){
            throw new RuntimeException("Unsupported invocation type " + lambdaProcessorConfig.getInvocationType());
        }

        invocationType = invocationTypeMap.get(lambdaProcessorConfig.getInvocationType());

        bufferedEventHandles = new LinkedList<>();

        lambdaAsyncClient = LambdaClientFactory.createAsyncLambdaClient(lambdaProcessorConfig.getAwsAuthenticationOptions(),
                lambdaProcessorConfig.getMaxConnectionRetries()
                , awsCredentialsSupplier,lambdaProcessorConfig.getSdkTimeout());

        this.bufferFactory = new InMemoryBufferFactory();

        Boolean isSink = false;
        PluginSetting pluginSetting = null;
        DlqPushHandler dlqPushHandler = null;

        LOG.info("LambdaFunctionName:{}, isBatchEnabled:{} , isSink:{}, invocationType:{}," +
                        "payload_model:{}",
                functionName,isBatchEnabled,isSink,invocationType,payloadModel);
        // Initialize LambdaCommonHandler
        lambdaCommonHandler = new LambdaCommonHandler(
                LOG,
                LambdaClientFactory.createAsyncLambdaClient(
                        lambdaProcessorConfig.getAwsAuthenticationOptions(),
                        lambdaProcessorConfig.getMaxConnectionRetries(),
                        awsCredentialsSupplier,
                        lambdaProcessorConfig.getSdkTimeout()),
                lambdaProcessorConfig.getFunctionName(),
                invocationTypeMap.get(lambdaProcessorConfig.getInvocationType()),
                expressionEvaluator,
                pluginMetrics.counter("lambdaProcessorObjectsEventsSucceeded"),
                pluginMetrics.counter("lambdaProcessorObjectsEventsFailed"),
                pluginMetrics.timer("lambdaProcessorLatency"),
                new InMemoryBufferFactory(),
                codec,
                codecContext,
                isBatchEnabled,
                whenCondition,
                maxEvents,
                maxBytes,
                maxCollectionDuration,
                isSink,
                dlqPushHandler,
                pluginSetting);
    }

    @Override
    public Collection<Record<Event>> doExecute(Collection<Record<Event>> records) {
        if (records.isEmpty()) {
            return records;
        }

        //lambda mutates event
        List<Record<Event>> resultRecords = new ArrayList<>();

        for (Record<Event> record : records) {
            final Event event = record.getData();

            if (whenCondition != null && !expressionEvaluator.evaluateConditional(whenCondition, event)) {
                continue;
            }

            try {
               lambdaCommonHandler.processEvent(resultRecords, event);
            } catch (Exception e) {
                LOG.atError()
                        .addMarker(EVENT)
                        .addMarker(NOISY)
                        .setMessage("There was an exception while processing Event [{}]")
                        .setCause(e)
                        .log();
                lambdaCommonHandler.handleFailure(e);
                lambdaCommonHandler.resetBuffer();
            }
        }

        LOG.debug("Force Flushing the remaining {} events in the buffer", lambdaCommonHandler.getCurrentBuffer().getEventCount());
        // Flush any remaining events in the buffer after processing all records
        if (lambdaCommonHandler.getCurrentBuffer().getEventCount() > 0) {
            try {
                lambdaCommonHandler.flushToLambdaIfNeeded(resultRecords, true); // Force flush remaining events
            } catch (Exception e) {
                LOG.error("Exception while flushing remaining events", e);
            }
        }

        lambdaCommonHandler.waitForFutures();

        return resultRecords;
    }

    @Override
    public void prepareForShutdown() {

    }

    @Override
    public boolean isReadyForShutdown() {
        return false;
    }

    @Override
    public void shutdown() {

    }

}