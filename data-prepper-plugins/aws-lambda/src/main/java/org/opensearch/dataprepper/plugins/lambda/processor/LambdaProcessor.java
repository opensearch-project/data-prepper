/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.processor;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
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
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodecConfig;
import org.opensearch.dataprepper.plugins.lambda.common.LambdaCommonHandler;
import org.opensearch.dataprepper.plugins.lambda.common.ResponseEventHandlingStrategy;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.client.LambdaClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

@DataPrepperPlugin(name = "aws_lambda", pluginType = Processor.class, pluginConfigurationType = LambdaProcessorConfig.class)
public class LambdaProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {

  public static final String NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_SUCCESS = "lambdaProcessorObjectsEventsSucceeded";
  public static final String NUMBER_OF_RECORDS_FLUSHED_TO_LAMBDA_FAILED = "lambdaProcessorObjectsEventsFailed";
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
  private final Timer lambdaLatencyMetric;
  private final List<String> tagsOnMatchFailure;
  private final LambdaAsyncClient lambdaAsyncClient;
  private final AtomicLong requestPayloadMetric;
  private final AtomicLong responsePayloadMetric;
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
    this.lambdaLatencyMetric = pluginMetrics.timer(LAMBDA_LATENCY_METRIC);
    this.requestPayloadMetric = pluginMetrics.gauge(REQUEST_PAYLOAD_SIZE, new AtomicLong());
    this.responsePayloadMetric = pluginMetrics.gauge(RESPONSE_PAYLOAD_SIZE, new AtomicLong());
    this.whenCondition = lambdaProcessorConfig.getWhenCondition();

    tagsOnMatchFailure = lambdaProcessorConfig.getTagsOnMatchFailure();

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

    lambdaAsyncClient = LambdaClientFactory.createAsyncLambdaClient(
        lambdaProcessorConfig.getAwsAuthenticationOptions(),
        lambdaProcessorConfig.getMaxConnectionRetries(), awsCredentialsSupplier,
        lambdaProcessorConfig.getConnectionTimeout());

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
    // Setup request codec

    return LambdaCommonHandler.sendRecords(records,
        whenCondition,
        expressionEvaluator,
        lambdaProcessorConfig,
        lambdaAsyncClient,
        (inputBuffer, invokeResponse) -> {
          convertLambdaResponseToEvent(inputBuffer, invokeResponse);
        },
        (inputBuffer, resultRecords) -> {
          addFailureTags(inputBuffer, resultRecords);
        });
  }

  /*
   * Assumption: Lambda always returns json array.
   * 1. If response has an array, we assume that we split the individual events.
   * 2. If it is not an array, then create one event per response.
   */
  private void convertLambdaResponseToEvent(Buffer flushedBuffer, InvokeResponse lambdaResponse) {
    InputCodec responseCodec = pluginFactory.loadPlugin(InputCodec.class, codecPluginSetting);
    try {
      List<Event> parsedEvents = new ArrayList<>();
      List<Record<Event>> originalRecords = flushedBuffer.getRecords();

      SdkBytes payload = lambdaResponse.payload();
      // Handle null or empty payload
      if (payload == null || payload.asByteArray() == null || payload.asByteArray().length == 0) {
        LOG.warn(NOISY, "Lambda response payload is null or empty, dropping the original events");
        // Set metrics
        //requestPayloadMetric.set(flushedBuffer.getPayloadRequestSize());
        //responsePayloadMetric.set(0);
      } else {
        // Set metrics
        //requestPayloadMetric.set(flushedBuffer.getPayloadRequestSize());
        //responsePayloadMetric.set(payload.asByteArray().length);

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
        /*synchronized (resultRecords) {
          responseStrategy.handleEvents(parsedEvents, originalRecords, resultRecords,
              flushedBuffer);
        }*/
      }
    } catch (Exception e) {
      LOG.error(NOISY, "Error converting Lambda response to Event");
      // Metrics update
      //requestPayloadMetric.set(flushedBuffer.getPayloadRequestSize());
      //responsePayloadMetric.set(0);
      //????? handleFailure(e, flushedBuffer, resultRecords, failureHandler);
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
