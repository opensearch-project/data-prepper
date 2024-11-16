/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.sink;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
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
import org.opensearch.dataprepper.plugins.lambda.common.LambdaCommonHandler;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.client.LambdaClientFactory;
import org.opensearch.dataprepper.plugins.lambda.sink.dlq.DlqPushHandler;
import org.opensearch.dataprepper.plugins.lambda.sink.dlq.LambdaSinkFailedDlqData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;

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
    this.numberOfRecordsSuccessCounter = pluginMetrics.counter(
        NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_SUCCESS);
    this.numberOfRecordsFailedCounter = pluginMetrics.counter(
        NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_FAILED);
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

    //Result from lambda is not currently processes.
    LambdaCommonHandler.sendRecords(records,
        lambdaSinkConfig,
        lambdaAsyncClient,
        (inputBuffer, invokeResponse) -> {
          releaseEventHandlesPerBatch(true, inputBuffer);
          return null;
        },
        (inputBuffer, invokeResponse) -> {
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
        dlqPushHandler.perform(pluginSetting,
            new LambdaSinkFailedDlqData(payload, throwable.getMessage(), 0));
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
      if (event != null) {
        EventHandle eventHandle = event.getEventHandle();
        if (eventHandle != null) {
          eventHandle.release(success);
        }
      }
    }
  }
}