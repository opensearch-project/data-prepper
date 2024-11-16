/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.common;

<<<<<<< HEAD
=======
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;

import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.BufferFactory;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.InMemoryBufferFactory;
import org.opensearch.dataprepper.plugins.lambda.common.util.ThresholdCheck;
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodec;
import org.opensearch.dataprepper.plugins.codec.json.JsonOutputCodecConfig;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.types.ByteCount;

import org.slf4j.Logger;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.io.IOException;
>>>>>>> 1628b8e88 (Checkstyle cleanup)
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.InMemoryBuffer;
import org.opensearch.dataprepper.plugins.lambda.common.config.BatchOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig;
import org.opensearch.dataprepper.plugins.lambda.common.util.ThresholdCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

public class LambdaCommonHandler {

  private static final Logger LOG = LoggerFactory.getLogger(LambdaCommonHandler.class);
  private LambdaCommonHandler() {
  }

  public static boolean isSuccess(InvokeResponse response) {
    int statusCode = response.statusCode();
    if (statusCode < 200 || statusCode >= 300) {
      return false;
    }
    return true;
  }

  public static void waitForFutures(List<CompletableFuture<InvokeResponse>> futureList) {

    if (!futureList.isEmpty()) {
      try {
        CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0])).join();
      } catch (Exception e) {
        LOG.warn("Exception while waiting for Lambda invocations to complete", e);
      }
    }
  }

  private static List<Buffer> createBufferBatches(Collection<Record<Event>> records,
      BatchOptions batchOptions, final OutputCodecContext outputCodecContext) {

    int maxEvents = batchOptions.getThresholdOptions().getEventCount();
    ByteCount maxBytes = batchOptions.getThresholdOptions().getMaximumSize();
    String keyName = batchOptions.getKeyName();
    Duration maxCollectionDuration = batchOptions.getThresholdOptions().getEventCollectTimeOut();

    Buffer currentBufferPerBatch = new InMemoryBuffer(keyName, outputCodecContext);
    List<Buffer> batchedBuffers = new ArrayList<>();

    LOG.debug("Batch size received to lambda processor: {}", records.size());
    for (Record<Event> record : records) {

      currentBufferPerBatch.addRecord(record);
      if (ThresholdCheck.checkThresholdExceed(currentBufferPerBatch, maxEvents, maxBytes,
          maxCollectionDuration)) {
        batchedBuffers.add(currentBufferPerBatch);
        currentBufferPerBatch = new InMemoryBuffer(keyName, outputCodecContext);
      }
    }

    if (currentBufferPerBatch.getEventCount() > 0) {
      batchedBuffers.add(currentBufferPerBatch);
    }
    return batchedBuffers;
  }

  public static List<Record<Event>> sendRecords(Collection<Record<Event>> records,
      LambdaCommonConfig config,
      LambdaAsyncClient lambdaAsyncClient,
      final OutputCodecContext outputCodecContext,
      BiFunction<Buffer, InvokeResponse, List<Record<Event>>> successHandler,
      Function<Buffer, List<Record<Event>>> failureHandler) {
    // Initialize here to void multi-threading issues
    // Note: By default, one instance of processor is created across threads.
    //List<Record<Event>> resultRecords = Collections.synchronizedList(new ArrayList<>());
    List<Record<Event>> resultRecords = new ArrayList<>();
    List<CompletableFuture<InvokeResponse>> futureList = new ArrayList<>();
    int totalFlushedEvents = 0;

    List<Buffer> batchedBuffers = createBufferBatches(records, config.getBatchOptions(),
        outputCodecContext);

    Map<Buffer, CompletableFuture> bufferToFutureMap = new HashMap<>();
    LOG.debug("Batch Chunks created after threshold check: {}", batchedBuffers.size());
    for (Buffer buffer : batchedBuffers) {
      InvokeRequest requestPayload = buffer.getRequestPayload(config.getFunctionName(),
              config.getInvocationType().getAwsLambdaValue());
      CompletableFuture<InvokeResponse> future = lambdaAsyncClient.invoke(requestPayload);
      futureList.add(future);
      bufferToFutureMap.put(buffer, future);
    }
    waitForFutures(futureList);
    for (Map.Entry<Buffer, CompletableFuture> entry : bufferToFutureMap.entrySet()) {
      CompletableFuture future = entry.getValue();
      Buffer buffer = entry.getKey();
      try {
        InvokeResponse response = (InvokeResponse) future.join();
        if (isSuccess(response)) {
          resultRecords.addAll(successHandler.apply(buffer, response));
        } else {
          LOG.error("Lambda invoke failed with error {} ", response.statusCode());
          resultRecords.addAll(failureHandler.apply(buffer));
        }
      } catch (Exception e) {
        LOG.error("Exception from Lambda invocation ", e);
        resultRecords.addAll(failureHandler.apply(buffer));
      }
    }
    return resultRecords;

  }

}
