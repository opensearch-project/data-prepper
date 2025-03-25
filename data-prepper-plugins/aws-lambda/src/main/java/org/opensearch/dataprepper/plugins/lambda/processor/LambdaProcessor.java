/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.processor;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.breaker.CircuitBreaker;
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
import static org.opensearch.dataprepper.plugins.lambda.common.LambdaCommonHandler.isSuccess;
import org.opensearch.dataprepper.plugins.lambda.common.ResponseEventHandlingStrategy;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.client.LambdaClientFactory;
import org.opensearch.dataprepper.plugins.lambda.common.config.ClientOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.services.lambda.model.LambdaException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@DataPrepperPlugin(name = "aws_lambda", pluginType = Processor.class, pluginConfigurationType = LambdaProcessorConfig.class)
public class LambdaProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {

    public static final String NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_SUCCESS = "recordsSuccessfullySentToLambda";
    public static final String NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_FAILED = "recordsFailedToSentLambda";
    public static final String NUMBER_OF_SUCCESSFUL_REQUESTS_TO_LAMBDA = "numberOfRequestsSucceeded";
    public static final String NUMBER_OF_FAILED_REQUESTS_TO_LAMBDA = "numberOfRequestsFailed";
    public static final String LAMBDA_LATENCY_METRIC = "lambdaFunctionLatency";
    public static final String REQUEST_PAYLOAD_SIZE = "requestPayloadSize";
    public static final String RESPONSE_PAYLOAD_SIZE = "responsePayloadSize";
    public static final String LAMBDA_RESPONSE_RECORDS_COUNTER = "lambdaResponseRecordsCounter";
    public static final String RECORDS_EXCEEDING_THRESHOLD = "recordsExceedingThreshold";
    public static final String CIRCUIT_BREAKER_TRIPS = "circuitBreakerTrips";
    private static final String NO_RETURN_RESPONSE = "null";
    private static final String EXCEEDING_PAYLOAD_LIMIT_EXCEPTION = "Status Code: 413";

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
    private final Counter lambdaResponseRecordsCounter;
    private final Counter batchExceedingThresholdCounter;
    private final Timer lambdaLatencyMetric;
    private final List<String> tagsOnFailure;
    private final LambdaAsyncClient lambdaAsyncClient;
    private final DistributionSummary requestPayloadMetric;
    private final DistributionSummary responsePayloadMetric;
    private final ResponseEventHandlingStrategy responseStrategy;
    private final JsonOutputCodecConfig jsonOutputCodecConfig;
    private final CircuitBreaker circuitBreaker;

    @DataPrepperPluginConstructor
    public LambdaProcessor(final PluginFactory pluginFactory, final PluginSetting pluginSetting,
                           final LambdaProcessorConfig lambdaProcessorConfig,
                           final AwsCredentialsSupplier awsCredentialsSupplier,
                           final ExpressionEvaluator expressionEvaluator,
                           final CircuitBreaker circuitBreaker) {
        super(
                PluginMetrics.fromPluginSetting(pluginSetting, pluginSetting.getName() + "_processor"));

        this.expressionEvaluator = expressionEvaluator;
        this.pluginFactory = pluginFactory;
        this.lambdaProcessorConfig = lambdaProcessorConfig;
        //Mostly used with HeapCircuitBreaker
        this.circuitBreaker = circuitBreaker;
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
        this.lambdaResponseRecordsCounter = pluginMetrics.counter(LAMBDA_RESPONSE_RECORDS_COUNTER);
        this.batchExceedingThresholdCounter = pluginMetrics.counter(RECORDS_EXCEEDING_THRESHOLD);

        this.whenCondition = lambdaProcessorConfig.getWhenCondition();
        this.tagsOnFailure = lambdaProcessorConfig.getTagsOnFailure();

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
        if (clientOptions == null) {
            clientOptions = new ClientOptions();
        }
        lambdaAsyncClient = LambdaClientFactory.createAsyncLambdaClient(
                lambdaProcessorConfig.getAwsAuthenticationOptions(),
                awsCredentialsSupplier,
                clientOptions
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

        List<Record<Event>> resultRecords = new ArrayList<>();
        List<Record<Event>> recordsToLambda = new ArrayList<>();
        for (Record<Event> record : records) {
            final Event event = record.getData();
            // If the condition is false, add the event to resultRecords as-is
            if (whenCondition != null && !expressionEvaluator.evaluateConditional(whenCondition,
                    event)) {
                resultRecords.add(record);
                continue;
            }
            recordsToLambda.add(record);
        }

        Map<Buffer, CompletableFuture<InvokeResponse>> bufferToFutureMap = new HashMap<>();
        try {
            // Check if circuit breaker is open - if so, wait until it closes
            checkCircuitBreaker();
            bufferToFutureMap = LambdaCommonHandler.sendRecords(
                    recordsToLambda, lambdaProcessorConfig, lambdaAsyncClient,
                    new OutputCodecContext());
        } catch (Exception e) {
            //NOTE: Ideally we should never hit this at least due to lambda invocation failure.
            // All lambda exceptions will reflect only when handling future.join() per request
            LOG.error(NOISY, "Error while batching and sending records to Lambda", e);
            numberOfRecordsFailedCounter.increment(recordsToLambda.size());
            numberOfRequestsFailedCounter.increment();
            resultRecords.addAll(addFailureTags(recordsToLambda));
        }

        for (Map.Entry<Buffer, CompletableFuture<InvokeResponse>> entry : bufferToFutureMap.entrySet()) {
            CompletableFuture<InvokeResponse> future = entry.getValue();
            Buffer inputBuffer = entry.getKey();
            try {
                InvokeResponse response = future.join();
                Duration latency = inputBuffer.stopLatencyWatch();
                lambdaLatencyMetric.record(latency.toMillis(), TimeUnit.MILLISECONDS);
                requestPayloadMetric.record(inputBuffer.getPayloadRequestSize());
                if (!isSuccess(response)) {
                    String errorMessage = String.format("Lambda invoke failed with status code %s error %s ",
                            response.statusCode(), response.payload().asUtf8String());
                    throw new RuntimeException(errorMessage);
                }

                resultRecords.addAll(convertLambdaResponseToEvent(inputBuffer, response));
                numberOfRecordsSuccessCounter.increment(inputBuffer.getEventCount());
                numberOfRequestsSuccessCounter.increment();
                if (response.payload() != null) {
                    responsePayloadMetric.record(response.payload().asByteArray().length);
                }

            } catch (Exception e) {
                LOG.error(NOISY, e.getMessage(), e);
                if (e instanceof LambdaException &&
                        e.getMessage() != null &&
                        e.getMessage().contains(EXCEEDING_PAYLOAD_LIMIT_EXCEPTION)) {
                    batchExceedingThresholdCounter.increment();
                }
                /* fall through */
                numberOfRecordsFailedCounter.increment(inputBuffer.getEventCount());
                numberOfRequestsFailedCounter.increment();
                resultRecords.addAll(addFailureTags(inputBuffer.getRecords()));
            }
        }
        return resultRecords;
    }

    private void checkCircuitBreaker() {
        if (circuitBreaker!=null && circuitBreaker.isOpen()) {
            LOG.warn("Circuit breaker is open. Will wait up to {} retries with {}ms interval before proceeding.",
                    lambdaProcessorConfig.getCircuitBreakerRetries(),
                    lambdaProcessorConfig.getCircuitBreakerWaitInterval());

            // Wait until the circuit breaker is closed
            int retries = 0;
            while (circuitBreaker.isOpen() && retries < lambdaProcessorConfig.getCircuitBreakerRetries()) {
                try {
                    LOG.warn(NOISY, "Circuit breaker is open," +
                                "Retry count: {}/{}", retries + 1, lambdaProcessorConfig.getCircuitBreakerRetries());
                    // Sleep for a short time before checking again
                    Thread.sleep(lambdaProcessorConfig.getCircuitBreakerWaitInterval());
                    retries++;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.warn("Interrupted while waiting for circuit breaker to close", e);
                    break;
                }
            }
            if (circuitBreaker.isOpen()) {
                LOG.warn("Proceeding with Lambda invocation after {} retries, even though circuit breaker is still open. " +
                        "This may lead to increased memory pressure.", retries);
            } else {
                LOG.info("Circuit breaker closed after {} retries. Resuming Lambda invocation.", retries);
            }
        }
    }

    /*
     * Assumption: Lambda always returns json array.
     * 1. If response has an array, we assume that we split the individual events.
     * 2. If it is not an array, then create one event per response.
     */
    List<Record<Event>> convertLambdaResponseToEvent(Buffer flushedBuffer,
                                                     final InvokeResponse lambdaResponse) throws IOException {
        InputCodec responseCodec = pluginFactory.loadPlugin(InputCodec.class, codecPluginSetting);
        List<Record<Event>> originalRecords = flushedBuffer.getRecords();

        List<Event> parsedEvents = new ArrayList<>();

        SdkBytes payload = lambdaResponse.payload();
        // Considering "null" payload as empty response from lambda and not parsing it.
        if (!(NO_RETURN_RESPONSE.equals(payload.asUtf8String()))) {
            //Convert using response codec
            InputStream inputStream = new ByteArrayInputStream(payload.asByteArray());
            responseCodec.parse(inputStream, record -> {
                Event event = record.getData();
                parsedEvents.add(event);
            });
        }
        LOG.debug("Parsed Event Size:{}, FlushedBuffer eventCount:{}, " +
                        "FlushedBuffer size:{}", parsedEvents.size(), flushedBuffer.getEventCount(),
                flushedBuffer.getSize());
        lambdaResponseRecordsCounter.increment(parsedEvents.size());
        return responseStrategy.handleEvents(parsedEvents, originalRecords);
    }

    /*
     * If one event in the Buffer fails, we consider that the entire
     * Batch fails and tag each event in that Batch.
     */
    private List<Record<Event>> addFailureTags(List<Record<Event>> records) {
        if (tagsOnFailure == null || tagsOnFailure.isEmpty()) {
            return records;
        }
        // Add failure tags to each event in the batch
        for (Record<Event> record : records) {
            Event event = record.getData();
            EventMetadata metadata = event.getMetadata();
            if (metadata != null) {
                metadata.addTags(tagsOnFailure);
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