/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.lambda.processor;

import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.plugins.lambda.common.LambdaCommonHandler;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.services.lambda.model.LambdaException;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;
import static org.opensearch.dataprepper.plugins.lambda.common.LambdaCommonHandler.isSuccess;

/**
 * Lambda invoker implementation for synchronous invocations.
 */
public class SyncLambdaInvoker implements LambdaInvoker {
    private static final Logger LOG = LoggerFactory.getLogger(SyncLambdaInvoker.class);
    private static final String EXCEEDING_PAYLOAD_LIMIT_EXCEPTION = "Status Code: 413";

    private final LambdaProcessor processor;

    public SyncLambdaInvoker(
            final LambdaProcessorConfig lambdaProcessorConfig,
            final LambdaAsyncClient lambdaAsyncClient,
            final InputCodec responseCodec,
            final LambdaProcessor processor) {
        this.processor = processor;
    }

    @Override
    public Collection<Record<Event>> invoke(List<Record<Event>> recordsToLambda, List<Record<Event>> resultRecords) {
        Map<Buffer, CompletableFuture<InvokeResponse>> bufferToFutureMap = new HashMap<>();
        try {
            bufferToFutureMap = LambdaCommonHandler.sendRecords(
                    recordsToLambda, 
                    processor.lambdaProcessorConfig, 
                    processor.lambdaAsyncClient,
                    new OutputCodecContext());
        } catch (Exception e) {
            //NOTE: Ideally we should never hit this at least due to lambda invocation failure.
            // All lambda exceptions will reflect only when handling future.join() per request
            LOG.error(NOISY, "Error while batching and sending records to Lambda", e);
            processor.numberOfRecordsFailedCounter.increment(recordsToLambda.size());
            processor.numberOfRequestsFailedCounter.increment();
            resultRecords.addAll(processor.addFailureTags(recordsToLambda));
        }

        for (Map.Entry<Buffer, CompletableFuture<InvokeResponse>> entry : bufferToFutureMap.entrySet()) {
            CompletableFuture<InvokeResponse> future = entry.getValue();
            Buffer inputBuffer = entry.getKey();
            try {
                InvokeResponse response = future.join();
                Duration latency = inputBuffer.stopLatencyWatch();
                processor.lambdaLatencyMetric.record(latency.toMillis(), TimeUnit.MILLISECONDS);
                processor.requestPayloadMetric.record(inputBuffer.getPayloadRequestSize());
                if (!isSuccess(response)) {
                    String errorMessage = String.format("Lambda invoke failed with status code %s error %s ",
                            response.statusCode(), response.payload().asUtf8String());
                    throw new RuntimeException(errorMessage);
                }

                resultRecords.addAll(processor.convertLambdaResponseToEvent(inputBuffer, response));
                processor.numberOfRecordsSuccessCounter.increment(inputBuffer.getEventCount());
                processor.numberOfRequestsSuccessCounter.increment();
                if (response.payload() != null) {
                    processor.responsePayloadMetric.record(response.payload().asByteArray().length);
                }

            } catch (Exception e) {
                LOG.error(NOISY, e.getMessage(), e);
                if (e instanceof LambdaException &&
                        e.getMessage() != null &&
                        e.getMessage().contains(EXCEEDING_PAYLOAD_LIMIT_EXCEPTION)) {
                    processor.batchExceedingThresholdCounter.increment();
                }
                /* fall through */
                processor.numberOfRecordsFailedCounter.increment(inputBuffer.getEventCount());
                processor.numberOfRequestsFailedCounter.increment();
                resultRecords.addAll(processor.addFailureTags(inputBuffer.getRecords()));
            }
        }
        return resultRecords;
    }
}
