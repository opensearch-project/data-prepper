/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.sink;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
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
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.InMemoryBufferSynchronized;
import org.opensearch.dataprepper.plugins.lambda.common.client.LambdaClientFactory;
import org.opensearch.dataprepper.plugins.lambda.common.config.ClientOptions;
import org.opensearch.dataprepper.plugins.lambda.common.util.ThresholdCheck;
import org.opensearch.dataprepper.plugins.lambda.sink.dlq.DlqPushHandler;
import org.opensearch.dataprepper.plugins.lambda.sink.dlq.LambdaSinkFailedDlqData;
import org.opensearch.dataprepper.model.failures.DlqObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.net.HttpURLConnection;
import java.time.Duration;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;
import static org.opensearch.dataprepper.plugins.lambda.common.LambdaCommonHandler.isSuccess;

@DataPrepperPlugin(name = "aws_lambda", pluginType = Sink.class, pluginConfigurationType = LambdaSinkConfig.class)
public class LambdaSink extends AbstractSink<Record<Event>> {

    public static final String NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_SUCCESS = "recordsSuccessfullySentToLambda";
    public static final String NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_FAILED = "recordsFailedToSentLambda";
    public static final String NUMBER_OF_SUCCESSFUL_REQUESTS_TO_LAMBDA = "numberOfRequestsSucceeded";
    public static final String NUMBER_OF_FAILED_REQUESTS_TO_LAMBDA = "numberOfRequestsFailed";
    public static final String LAMBDA_LATENCY_METRIC = "lambdaFunctionLatency";
    public static final String REQUEST_PAYLOAD_SIZE = "requestPayloadSize";
    public static final String RESPONSE_PAYLOAD_SIZE = "responsePayloadSize";

    private static final Logger LOG = LoggerFactory.getLogger(LambdaSink.class);
    private static final String BUCKET = "bucket";
    private static final String KEY_PATH = "key_path_prefix";
    private final Counter numberOfRecordsSuccessCounter;
    private final Counter numberOfRecordsFailedCounter;
    private final Counter numberOfRequestsSuccessCounter;
    private final Counter numberOfRequestsFailedCounter;
    private final LambdaSinkConfig lambdaSinkConfig;
    private final ExpressionEvaluator expressionEvaluator;
    private final LambdaAsyncClient lambdaAsyncClient;
    private final DistributionSummary responsePayloadMetric;
    private final Timer lambdaLatencyMetric;
    private final DistributionSummary requestPayloadMetric;
    private final PluginSetting pluginSetting;
    private final OutputCodecContext outputCodecContext;
    private volatile boolean sinkInitialized;
    private DlqPushHandler dlqPushHandler = null;
    final int maxEvents;
    final ByteCount maxBytes;
    final Duration maxCollectTime;

    // The partial buffer that may not yet have reached threshold.
    // Access must be synchronized
    private Buffer statefulBuffer;
    private ReentrantLock reentrantLock;

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
        this.outputCodecContext = OutputCodecContext.fromSinkContext(sinkContext);
        this.maxEvents = lambdaSinkConfig.getBatchOptions().getThresholdOptions().getEventCount();
        this.maxBytes = lambdaSinkConfig.getBatchOptions().getThresholdOptions().getMaximumSize();
        this.maxCollectTime = lambdaSinkConfig.getBatchOptions().getThresholdOptions().getEventCollectTimeOut();

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
        ClientOptions clientOptions = lambdaSinkConfig.getClientOptions();
        if (clientOptions == null) {
            clientOptions = new ClientOptions();
        }
        this.lambdaAsyncClient = LambdaClientFactory.createAsyncLambdaClient(
                lambdaSinkConfig.getAwsAuthenticationOptions(),
                awsCredentialsSupplier,
                clientOptions
        );
        if (lambdaSinkConfig.getDlqPluginSetting() != null) {
            this.dlqPushHandler = new DlqPushHandler(pluginFactory, pluginSetting, lambdaSinkConfig.getDlq(), lambdaSinkConfig.getAwsAuthenticationOptions());
        }
        reentrantLock = new ReentrantLock();

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
        // Initialize the partial buffer
        statefulBuffer = new InMemoryBufferSynchronized(
                lambdaSinkConfig.getBatchOptions().getKeyName(),
                outputCodecContext
        );
        sinkInitialized = Boolean.TRUE;
    }

    /**
     * We only flush the partial buffer if we're shutting down or if we want to
     * do a time-based flush.
     */
    @Override
    public void shutdown() {
        // Flush the partial buffer if any leftover
        if (statefulBuffer.getEventCount() > 0) {
            flushBuffers(Collections.singletonList(statefulBuffer));
        }
    }

    @Override
    public void doOutput(final Collection<Record<Event>> records) {
        if (!sinkInitialized) {
            LOG.warn("LambdaSink doOutput called before initialization");
            return;
        }
        reentrantLock.lock();
        try {
            //check if old buffer needs to be flushed
            if (statefulBuffer.getEventCount() > 0
                    && ThresholdCheck.checkTimeoutExceeded(statefulBuffer, maxCollectTime)) {
                LOG.debug("Flushing partial buffer due to timeout of {}", maxCollectTime);
                final Buffer bufferToFlush = statefulBuffer;
                // create a new partial buffer.
                statefulBuffer = new InMemoryBufferSynchronized(
                        lambdaSinkConfig.getBatchOptions().getKeyName(),
                        outputCodecContext
                );
                flushBuffers(Collections.singletonList(bufferToFlush));
            }

            // We'll collect any "full" buffers in a local list, flush them at the end
            List<Buffer> fullBuffers = new ArrayList<>();

            // Add to the persistent buffer, check threshold
            for (Record<Event> record : records) {
                if (ThresholdCheck.checkSizeThresholdExceed(statefulBuffer, maxBytes, record)
                || ThresholdCheck.checkTimeoutExceeded(statefulBuffer, maxCollectTime)) {
                    fullBuffers.add(statefulBuffer);
                    statefulBuffer = new InMemoryBufferSynchronized(lambdaSinkConfig.getBatchOptions().getKeyName());
                }

                //statefulBuffer is either empty or partially filled(from previous run)
                statefulBuffer.addRecord(record);

                if (ThresholdCheck.checkEventCountThresholdExceeded(statefulBuffer, maxEvents)) {
                    // This buffer is full
                    fullBuffers.add(statefulBuffer);
                    // Create new partial buffer
                    statefulBuffer = new InMemoryBufferSynchronized(
                            lambdaSinkConfig.getBatchOptions().getKeyName(),
                            outputCodecContext
                    );
                }
            }

            // Flush any full buffers
            if (!fullBuffers.isEmpty()) {
                flushBuffers(fullBuffers);
            }
        } finally {
            reentrantLock.unlock();
        }
    }


  private DlqObject createDlqObjectFromEvent(final Event event,
                                             final String functionName,
                                             final int status,
                                             final String message) {
    return DlqObject.builder()
            .withEventHandle(event.getEventHandle())
            .withFailedData(LambdaSinkFailedDlqData.builder()
                    .withData(event.toJsonString())
                    .withStatus(status)
                    .withFunctionName(functionName)
                    .withMessage(message)
                    .build())
            .withPluginName(pluginSetting.getName())
            .withPipelineName(pluginSetting.getPipelineName())
            .withPluginId(pluginSetting.getName())
            .build();
  }

  void handleFailure(Collection<Record<Event>> failedRecords, Throwable throwable, int statusCode) {
    if (failedRecords.isEmpty()) {
        return;
    }
    numberOfRecordsFailedCounter.increment(failedRecords.size());
    numberOfRequestsFailedCounter.increment();
    if (dlqPushHandler == null) {
      releaseEventHandles(failedRecords, false);
    }
    try {
      final List<DlqObject> dlqObjects = new ArrayList<>();
      for (Record<Event> record: failedRecords) {
          if (record.getData() != null) {
              dlqObjects.add(createDlqObjectFromEvent(record.getData(), lambdaSinkConfig.getFunctionName(), statusCode, throwable.getMessage()));
          }
      }
      dlqPushHandler.perform(dlqObjects);
      releaseEventHandles(failedRecords, true);
    } catch (Exception ex) {
      LOG.error("Exception occured during error handling");
      releaseEventHandles(failedRecords, false);
    }
  }
    /*
     * Release events per batch
     */
    private void releaseEventHandles(Collection<Record<Event>> records, boolean success) {
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

    void flushBuffers(final List<Buffer> buffersToFlush) {


        Map<Buffer, CompletableFuture<InvokeResponse>> bufferToFutureMap;
        try {
            bufferToFutureMap = LambdaCommonHandler.invokeLambdaAndGetFutureMap(
                    lambdaSinkConfig,
                    lambdaAsyncClient,
                    buffersToFlush
            );
        } catch (Exception e) {
            LOG.error(NOISY, "Error sending buffers to Lambda", e);
            // Note: if there are N buffers and only N-1 buffers are full,
            // we only need to propagate N-1 buffer records as failure
            List<Record<Event>> combinedRecords = new ArrayList<>();
            for (Buffer buf : buffersToFlush) {
                combinedRecords.addAll(buf.getRecords());
            }
            handleFailure(combinedRecords, e, HttpURLConnection.HTTP_INTERNAL_ERROR);
            return;
        }

        for (Map.Entry<Buffer, CompletableFuture<InvokeResponse>> entry : bufferToFutureMap.entrySet()) {
            Buffer inputBuffer = entry.getKey();
            CompletableFuture<InvokeResponse> future = entry.getValue();

            try {
                InvokeResponse response = future.join();
                Duration latency = inputBuffer.stopLatencyWatch();
                lambdaLatencyMetric.record(latency.toMillis(), TimeUnit.MILLISECONDS);
                requestPayloadMetric.record(inputBuffer.getPayloadRequestSize());
                if (!isSuccess(response)) {
                    String errorMsg = String.format(
                            "Lambda invoke failed with code %d, error: %s",
                            response.statusCode(),
                            response.payload() != null ? response.payload().asUtf8String() : "No payload"
                    );
                    throw new RuntimeException(errorMsg);
                }

                releaseEventHandles(inputBuffer.getRecords(), true);
                numberOfRecordsSuccessCounter.increment(inputBuffer.getEventCount());
                numberOfRequestsSuccessCounter.increment();
                if (response.payload() != null) {
                    responsePayloadMetric.record(response.payload().asByteArray().length);
                }
            } catch (Exception ex) {
                LOG.error(NOISY, "Error handling future response from Lambda", ex);
                handleFailure(inputBuffer.getRecords(), ex, HttpURLConnection.HTTP_INTERNAL_ERROR);
            }
        }
    }

    private boolean isThresholdExceeded(Buffer buffer, Record record) {
        return ThresholdCheck.checkEventCountThresholdExceeded(
                buffer,
                maxEvents
        ) || ThresholdCheck.checkSizeThresholdExceed(
                buffer,
                maxBytes,
                record
        );
    }
}
