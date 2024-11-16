/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.common;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
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

  public static boolean checkStatusCode(InvokeResponse response) {
    int statusCode = response.statusCode();
    if (statusCode < 200 || statusCode >= 300) {
      LOG.error("Lambda invocation returned with non-success status code: {}", statusCode);
      return false;
    }
    return true;
  }

  public static void waitForFutures(List<CompletableFuture<InvokeResponse>> futureList) {
    if (!futureList.isEmpty()) {
      try {
        CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0])).join();
        LOG.info("All {} Lambda invocations have completed", futureList.size());
      } catch (Exception e) {
        LOG.warn("Exception while waiting for Lambda invocations to complete", e);
      } finally {
        futureList.clear();
      }
    }
  }

  private static List<Buffer> createBufferBatches(Collection<Record<Event>> records,
      String whenCondition,
      ExpressionEvaluator expressionEvaluator,
      BatchOptions batchOptions,
      List<Record<Event>> resultRecords) {

    int maxEvents = batchOptions.getThresholdOptions().getEventCount();
    ByteCount maxBytes = batchOptions.getThresholdOptions().getMaximumSize();
    String keyName = batchOptions.getKeyName();
    Duration maxCollectionDuration = batchOptions.getThresholdOptions().getEventCollectTimeOut();

    Buffer currentBufferPerBatch = new InMemoryBuffer(keyName);
    List<Buffer> batchedBuffers = new ArrayList<>();

    LOG.info("Batch size received to lambda processor: {}", records.size());
    for (Record<Event> record : records) {
      final Event event = record.getData();

      //only processor needs to execute this block
      if (resultRecords != null) {
        if (whenCondition != null && !expressionEvaluator.evaluateConditional(whenCondition,
            event)) {
          resultRecords.add(record);
          continue;
        }
      }

      currentBufferPerBatch.addRecord(record);
      if (ThresholdCheck.checkThresholdExceed(currentBufferPerBatch, maxEvents, maxBytes,
          maxCollectionDuration)) {
        batchedBuffers.add(currentBufferPerBatch);
        currentBufferPerBatch = new InMemoryBuffer(keyName);
      }
    }

    if (currentBufferPerBatch.getEventCount() > 0) {
      batchedBuffers.add(currentBufferPerBatch);
    }
    return batchedBuffers;
  }

  public static List<Record<Event>> sendRecords(Collection<Record<Event>> records,
      String whenCondition,
      ExpressionEvaluator expressionEvaluator,
      LambdaCommonConfig config,
      LambdaAsyncClient lambdaAsyncClient,
      BiConsumer<Buffer, InvokeResponse> successHandler,
      BiConsumer<Buffer, List<Record<Event>>> failureHandler) {
    // Initialize here to void multi-threading issues
    // Note: By default, one instance of processor is created across threads.
    List<Record<Event>> resultRecords = Collections.synchronizedList(new ArrayList<>());
    List<CompletableFuture<InvokeResponse>> futureList = new ArrayList<>();
    int totalFlushedEvents = 0;

    List<Buffer> batchedBuffers = createBufferBatches(records,
        whenCondition, expressionEvaluator, config.getBatchOptions(), resultRecords);

    LOG.info("Batch Chunks created after threshold check: {}", batchedBuffers.size());
    for (Buffer buffer : batchedBuffers) {
      InvokeRequest requestPayload = buffer.getRequestPayload(config.getFunctionName(),
          config.getInvocationType().getAwsLambdaValue());
      CompletableFuture<InvokeResponse> future = lambdaAsyncClient.invoke(requestPayload);
      futureList.add(future);
      future.thenAccept(response -> {
        synchronized (resultRecords) {
          successHandler.accept(buffer, response);
        }
      }).exceptionally(throwable -> {
        synchronized (resultRecords) {
          failureHandler.accept(buffer, resultRecords);
        }
        return null;
      });
      totalFlushedEvents += buffer.getEventCount();
    }

    waitForFutures(futureList);
    LOG.info("Total events flushed to lambda successfully: {}", totalFlushedEvents);
    return resultRecords;
  }

  /*
   * If one event in the Buffer fails, we consider that the entire
   * Batch fails and tag each event in that Batch.
   */
  static void handleFailure(Throwable e, Buffer flushedBuffer, List<Record<Event>> resultRecords,
      BiConsumer<Buffer, List<Record<Event>>> failureHandler) {
    if (flushedBuffer.getEventCount() > 0) {
      //numberOfRecordsFailedCounter.increment(flushedBuffer.getEventCount());
    }

    LOG.error(NOISY, "Failed to process batch due to error: ", e);

  }

}
