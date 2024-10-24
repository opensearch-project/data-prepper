/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.processor;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.EVENT;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodec;
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodecConfig;
import org.opensearch.dataprepper.plugins.codec.json.NdjsonOutputCodec;
import org.opensearch.dataprepper.plugins.codec.json.NdjsonOutputConfig;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.BufferFactory;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.InMemoryBufferFactory;
import org.opensearch.dataprepper.plugins.lambda.common.client.LambdaClientFactory;
import org.opensearch.dataprepper.plugins.lambda.common.config.BatchOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig;
import static org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig.BATCH_EVENT;
import static org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig.SINGLE_EVENT;
import static org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig.invocationTypeMap;
import org.opensearch.dataprepper.plugins.lambda.common.util.ThresholdCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
    private final List<Event> events;
    private final BatchOptions batchOptions;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final BufferFactory bufferFactory;
    private final LambdaClient lambdaClient;
    private final Boolean isBatchEnabled;
    Buffer currentBuffer;
    private final AtomicLong requestPayloadMetric;
    private final AtomicLong responsePayload;
    private String payloadModel = null;
    private int maxEvents = 0;
    private ByteCount maxBytes = null;
    private Duration maxCollectionDuration = null;
    private int maxRetries = 0;
    private OutputCodec codec = null;
    OutputCodecContext codecContext = null;

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
            LOG.info("maxEvents:" + maxEvents + " maxbytes:" + maxBytes + " maxDuration:" + maxCollectionDuration);
        } else if(payloadModel.equals(SINGLE_EVENT)) {
            NdjsonOutputConfig ndjsonOutputCodecConfig = new NdjsonOutputConfig();
            codec = new NdjsonOutputCodec(ndjsonOutputCodecConfig);
            isBatchEnabled = false;
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
        events = new ArrayList();

        lambdaClient = LambdaClientFactory.createLambdaClient(lambdaProcessorConfig.getAwsAuthenticationOptions(),
                lambdaProcessorConfig.getMaxConnectionRetries()
                , awsCredentialsSupplier);

        this.bufferFactory = new InMemoryBufferFactory();
        try {
            currentBuffer = this.bufferFactory.getBuffer(lambdaClient, functionName, invocationType);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

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
                if (currentBuffer.getEventCount() == 0) {
                    codec.start(currentBuffer.getOutputStream(), event, codecContext);
                }
                codec.writeEvent(event, currentBuffer.getOutputStream());
                int count = currentBuffer.getEventCount() + 1;
                currentBuffer.setEventCount(count);

                // flush to lambda and update result record
                flushToLambdaIfNeeded(resultRecords);
            } catch (Exception e) {
                numberOfRecordsFailedCounter.increment(currentBuffer.getEventCount());
                LOG.error(EVENT, "There was an exception while processing Event [{}]" , event, e);
                //reset buffer
                try {
                    currentBuffer = bufferFactory.getBuffer(lambdaClient, functionName, invocationType);
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
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

    protected void flushToLambdaIfNeeded(List<Record<Event>> resultRecords) throws InterruptedException, IOException {
        LOG.info("Flush to Lambda check: currentBuffer.size={}, currentBuffer.events={}, currentBuffer.duration={}", currentBuffer.getSize(), currentBuffer.getEventCount(), currentBuffer.getDuration());

        if (ThresholdCheck.checkThresholdExceed(currentBuffer, maxEvents, maxBytes, maxCollectionDuration, isBatchEnabled)) {
            try{
                codec.complete(currentBuffer.getOutputStream());
                responsePayload.set(currentBuffer.getPayloadResponseSyncSize());
                InvokeResponse lambdaResponse = currentBuffer.flushToLambdaSync();
                handleLambdaResponse(lambdaResponse);
                numberOfRecordsSuccessCounter.increment(currentBuffer.getEventCount());
                lambdaLatencyMetric.record(currentBuffer.getFlushLambdaSyncLatencyMetric());
                requestPayloadMetric.set(currentBuffer.getPayloadRequestSyncSize());
                Event lambdaEvent = convertLambdaResponseToEvent(lambdaResponse);
                resultRecords.add(new Record<>(lambdaEvent));
            } catch(AwsServiceException | SdkClientException e) {
                LOG.error(EVENT, "Exception occurred while uploading records to lambda. functionName: {} | exception:", functionName, e);
                numberOfRecordsFailedCounter.increment(currentBuffer.getEventCount());
            } catch (final IOException e) {
                LOG.error("Exception while completing codec", e);
                numberOfRecordsFailedCounter.increment(currentBuffer.getEventCount());
            }
            //Reset Buffer
            try {
                currentBuffer = bufferFactory.getBuffer(lambdaClient, functionName, invocationType);
            } catch (IOException ex) {
                throw new RuntimeException("Failed to reset buffer after exception", ex);
            }
        }
    }

    protected Event convertLambdaResponseToEvent(InvokeResponse lambdaResponse) {
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

    private void handleLambdaResponse(InvokeResponse response){
        int statusCode = response.statusCode();
        if (statusCode < 200 || statusCode >= 300) {
            throw new RuntimeException("Lambda invocation failed with status code: " + statusCode);
        }
    }
}