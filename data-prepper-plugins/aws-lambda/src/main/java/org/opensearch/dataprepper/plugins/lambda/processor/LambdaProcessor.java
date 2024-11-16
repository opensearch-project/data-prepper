/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.processor;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodecConfig;
import org.opensearch.dataprepper.plugins.lambda.common.LambdaCommonHandler;
import org.opensearch.dataprepper.plugins.lambda.common.ResponseEventHandlingStrategy;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.client.LambdaClientFactory;
import org.opensearch.dataprepper.plugins.lambda.common.config.ClientOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

@DataPrepperPlugin(name = "aws_lambda", pluginType = Processor.class, pluginConfigurationType = LambdaProcessorConfig.class)
public class LambdaProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {

    public static final String NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_SUCCESS = "lambdaProcessorObjectsEventsSucceeded";
    public static final String NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_FAILED = "lambdaProcessorObjectsEventsFailed";
    public static final String NUMBER_OF_SUCCESSFUL_REQUESTS_TO_LAMBDA = "lambdaProcessorNumberOfRequestsSucceeded";
    public static final String NUMBER_OF_FAILED_REQUESTS_TO_LAMBDA = "lambdaProcessorNumberOfRequestsFailed";
    public static final String LAMBDA_LATENCY_METRIC = "lambdaProcessorLatency";
    public static final String REQUEST_PAYLOAD_SIZE = "requestPayloadSize";
    public static final String RESPONSE_PAYLOAD_SIZE = "responsePayloadSize";

    private static final Logger LOG = LoggerFactory.getLogger(LambdaProcessor.class);
    final PluginSetting codecPluginSetting;
    final PluginFactory pluginFactory;
    final LambdaProcessorConfig lambdaProcessorConfig;
    private final String whenCondition;
    private final ExpressionEvaluator expressionEvaluator;
    private final Counter numberOfRecordsSuccessCounter;
    private final Counter numberOfRecordsFailedCounter;
    private final Counter numberOfRequestsSuccessCounter;
    private final Counter numberOfRequestsFailedCounter;
    private final Timer lambdaLatencyMetric;
    private final List<String> tagsOnMatchFailure;
    private final LambdaAsyncClient lambdaAsyncClient;
    private final DistributionSummary requestPayloadMetric;
    private final DistributionSummary responsePayloadMetric;
    private final ResponseEventHandlingStrategy responseStrategy;
    private final JsonOutputCodecConfig jsonOutputCodecConfig;
    LambdaCommonHandler lambdaCommonHandler;

    @DataPrepperPluginConstructor
    public LambdaProcessor(final PluginFactory pluginFactory, final PluginMetrics pluginMetrics,
                           final LambdaProcessorConfig lambdaProcessorConfig,
                           final AwsCredentialsSupplier awsCredentialsSupplier,
                           final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.expressionEvaluator = expressionEvaluator;
        this.pluginFactory = pluginFactory;
        this.lambdaProcessorConfig = lambdaProcessorConfig;
        this.numberOfRecordsSuccessCounter = pluginMetrics.counter(
                NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_SUCCESS);
        this.numberOfRecordsFailedCounter = pluginMetrics.counter(
                NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_FAILED);
        this.numberOfRequestsSuccessCounter = pluginMetrics.counter(
                NUMBER_OF_SUCCESSFUL_REQUESTS_TO_LAMBDA);
        this.numberOfRequestsFailedCounter = pluginMetrics.counter(
                NUMBER_OF_FAILED_REQUESTS_TO_LAMBDA);
        this.lambdaLatencyMetric = pluginMetrics.timer(LAMBDA_LATENCY_METRIC);
        this.requestPayloadMetric = pluginMetrics.summary(REQUEST_PAYLOAD_SIZE);
        this.responsePayloadMetric = pluginMetrics.summary(RESPONSE_PAYLOAD_SIZE);
        this.whenCondition = lambdaProcessorConfig.getWhenCondition();
        this.tagsOnMatchFailure = lambdaProcessorConfig.getTagsOnFailure();

        PluginModel responseCodecConfig = lambdaProcessorConfig.getResponseCodecConfig();

        if (responseCodecConfig == null) {
            // Default to JsonInputCodec with default settings
            codecPluginSetting = new PluginSetting("json", Collections.emptyMap());
        } else {
            codecPluginSetting = new PluginSetting(responseCodecConfig.getPluginName(),
                    responseCodecConfig.getPluginSettings());
        }

        jsonOutputCodecConfig = new JsonOutputCodecConfig();
        jsonOutputCodecConfig.setKeyName(lambdaProcessorConfig.getBatchOptions().getKeyName());

        ClientOptions clientOptions = lambdaProcessorConfig.getClientOptions();
        if(clientOptions == null){
            clientOptions = new ClientOptions();
        }
        lambdaAsyncClient = LambdaClientFactory.createAsyncLambdaClient(
                lambdaProcessorConfig.getAwsAuthenticationOptions(),
                awsCredentialsSupplier,
                lambdaProcessorConfig.getClientOptions()
        );

        // Select the correct strategy based on the configuration
        if (lambdaProcessorConfig.getResponseEventsMatch()) {
            this.responseStrategy = new StrictResponseEventHandlingStrategy();
        } else {
            this.responseStrategy = new AggregateResponseEventHandlingStrategy();
        }

    }

    @Override
    public Collection<Record<Event>> doExecute(Collection<Record<Event>> records) {
        if (records.isEmpty()) {
            return records;
        }

        List<Record<Event>> resultRecords = Collections.synchronizedList(new ArrayList());
        List<Record<Event>> recordsToLambda = new ArrayList<>();
        for (Record<Event> record : records) {
            final Event event = record.getData();
            // If the condition is false, add the event to resultRecords as-is
            if (whenCondition != null && !expressionEvaluator.evaluateConditional(whenCondition, event)) {
                resultRecords.add(record);
                continue;
            }
            recordsToLambda.add(record);
        }
        try {
            resultRecords.addAll(
                    lambdaCommonHandler.sendRecords(recordsToLambda, lambdaProcessorConfig, lambdaAsyncClient,
                            new OutputCodecContext(),
                            (inputBuffer, response) -> {
                                Duration latency = inputBuffer.stopLatencyWatch();
                                lambdaLatencyMetric.record(latency.toMillis(), TimeUnit.MILLISECONDS);
                                numberOfRecordsSuccessCounter.increment(inputBuffer.getEventCount());
                                numberOfRequestsSuccessCounter.increment();
                                return convertLambdaResponseToEvent(inputBuffer, response);
                            },
                            (inputBuffer) -> {
                                Duration latency = inputBuffer.stopLatencyWatch();
                                lambdaLatencyMetric.record(latency.toMillis(), TimeUnit.MILLISECONDS);
                                numberOfRecordsFailedCounter.increment(inputBuffer.getEventCount());
                                numberOfRequestsFailedCounter.increment();
                                return addFailureTags(inputBuffer.getRecords());
                            })

            );
        } catch (Exception e) {
            LOG.info("Exception in doExecute");
            numberOfRecordsFailedCounter.increment(recordsToLambda.size());
            resultRecords.addAll(addFailureTags(recordsToLambda));
        }
        return resultRecords;
    }

    /*
     * Assumption: Lambda always returns json array.
     * 1. If response has an array, we assume that we split the individual events.
     * 2. If it is not an array, then create one event per response.
     */
    List<Record<Event>> convertLambdaResponseToEvent(Buffer flushedBuffer,
                                                     final InvokeResponse lambdaResponse) {
        InputCodec responseCodec = pluginFactory.loadPlugin(InputCodec.class, codecPluginSetting);
        List<Record<Event>> originalRecords = flushedBuffer.getRecords();
        try {
            List<Event> parsedEvents = new ArrayList<>();

            List<Record<Event>> resultRecords = new ArrayList<>();
            SdkBytes payload = lambdaResponse.payload();
            // Handle null or empty payload
            if (payload == null || payload.asByteArray() == null || payload.asByteArray().length == 0) {
                LOG.warn(NOISY, "Lambda response payload is null or empty, dropping the original events");
            } else {
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
            return resultRecords;
        } catch (Exception e) {
            LOG.error(NOISY, "Error converting Lambda response to Event");
            addFailureTags(flushedBuffer.getRecords());
            return originalRecords;
        }
    }

    /*
     * If one event in the Buffer fails, we consider that the entire
     * Batch fails and tag each event in that Batch.
     */
    private List<Record<Event>> addFailureTags(List<Record<Event>> records) {
        // Add failure tags to each event in the batch
        for (Record<Event> record : records) {
            Event event = record.getData();
            EventMetadata metadata = event.getMetadata();
            if (metadata != null) {
                metadata.addTags(tagsOnMatchFailure);
            } else {
                LOG.warn("Event metadata is null, cannot add failure tags.");
            }
        }
        return records;
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
