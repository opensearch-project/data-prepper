/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.common;

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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class LambdaCommonHandler {

    private static final Logger LOG = LoggerFactory.getLogger(LambdaCommonHandler.class);

    private LambdaCommonHandler() {
    }

    public static boolean isSuccess(InvokeResponse response) {
        if(response == null) {
            return false;
        }
        int statusCode = response.statusCode();
        return statusCode >= 200 && statusCode < 300;
    }

    public static void waitForFutures(Collection<CompletableFuture<InvokeResponse>> futureList) {

        if (!futureList.isEmpty()) {
            CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0])).join();
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
            //check size or time has exceeded threshold
            if (ThresholdCheck.checkSizeThresholdExceed(currentBufferPerBatch, maxBytes, record)) {
                batchedBuffers.add(currentBufferPerBatch);
                currentBufferPerBatch = new InMemoryBuffer(keyName, outputCodecContext);
            }

            currentBufferPerBatch.addRecord(record);

            // After adding, check if the event count threshold is reached.
            if (ThresholdCheck.checkEventCountThresholdExceeded(currentBufferPerBatch, maxEvents)) {
                batchedBuffers.add(currentBufferPerBatch);
                currentBufferPerBatch = new InMemoryBuffer(keyName, outputCodecContext);
            }
        }
        if (currentBufferPerBatch.getEventCount() > 0) {
            batchedBuffers.add(currentBufferPerBatch);
        }
        return batchedBuffers;
    }

    public static Map<Buffer, CompletableFuture<InvokeResponse>> sendRecords(
            Collection<Record<Event>> records,
            LambdaCommonConfig config,
            LambdaAsyncClient lambdaAsyncClient,
            final OutputCodecContext outputCodecContext) {

        List<Buffer> batchedBuffers = createBufferBatches(records, config.getBatchOptions(),
                outputCodecContext);

        Map<Buffer, CompletableFuture<InvokeResponse>> bufferToFutureMap = invokeLambdaAndGetFutureMap(config, lambdaAsyncClient, batchedBuffers);
        return bufferToFutureMap;
    }

    public static Map<Buffer, CompletableFuture<InvokeResponse>> invokeLambdaAndGetFutureMap(LambdaCommonConfig config, LambdaAsyncClient lambdaAsyncClient, List<Buffer> batchedBuffers) {
        Map<Buffer, CompletableFuture<InvokeResponse>> bufferToFutureMap = new HashMap<>();
        LOG.debug("Batch Chunks created after threshold check: {}", batchedBuffers.size());
        for (Buffer buffer : batchedBuffers) {
            InvokeRequest requestPayload = buffer.getRequestPayload(config.getFunctionName(),
                    config.getInvocationType().getAwsLambdaValue());
            CompletableFuture<InvokeResponse> future = lambdaAsyncClient.invoke(requestPayload);
            bufferToFutureMap.put(buffer, future);
        }
        return bufferToFutureMap;
    }

}
