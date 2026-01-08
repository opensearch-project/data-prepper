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
import org.opensearch.dataprepper.plugins.lambda.common.StreamingLambdaHandler;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;

/**
 * Lambda invoker implementation for streaming response invocations.
 */
public class StreamingLambdaInvoker implements LambdaInvoker {
    private static final Logger LOG = LoggerFactory.getLogger(StreamingLambdaInvoker.class);
    
    /**
     * Estimated average size per record in bytes for response payload metrics.
     * Used when actual streaming response size cannot be precisely calculated.
     */
    private static final int ESTIMATED_BYTES_PER_RECORD = 1024;

    private final LambdaProcessor processor;
    private final StreamingLambdaHandler streamingHandler;

    public StreamingLambdaInvoker(
            final LambdaProcessorConfig lambdaProcessorConfig,
            final LambdaAsyncClient lambdaAsyncClient,
            final InputCodec responseCodec,
            final LambdaProcessor processor) {
        this.processor = processor;
        this.streamingHandler = new StreamingLambdaHandler(
                lambdaAsyncClient, 
                processor.pluginFactory, 
                responseCodec,
                lambdaProcessorConfig.getFunctionName(),
                lambdaProcessorConfig.getStreamingOptions());
    }

    @Override
    public Collection<Record<Event>> invoke(List<Record<Event>> recordsToLambda, List<Record<Event>> resultRecords) {
        // Create buffers for streaming
        Map<Buffer, CompletableFuture<InvokeResponse>> bufferMap;
        try {
            bufferMap = LambdaCommonHandler.sendRecords(
                    recordsToLambda, 
                    processor.lambdaProcessorConfig, 
                    processor.lambdaAsyncClient, 
                    new OutputCodecContext());
        } catch (Exception e) {
            LOG.error(NOISY, "Error creating buffers for streaming", e);
            processor.numberOfRecordsFailedCounter.increment(recordsToLambda.size());
            processor.numberOfRequestsFailedCounter.increment();
            resultRecords.addAll(processor.addFailureTags(recordsToLambda));
            return resultRecords;
        }
        
        // Process each buffer with streaming
        for (Map.Entry<Buffer, CompletableFuture<InvokeResponse>> entry : bufferMap.entrySet()) {
            Buffer inputBuffer = entry.getKey();
            
            try {
                CompletableFuture<List<Record<Event>>> streamingFuture = streamingHandler.invokeWithStreaming(
                        inputBuffer
                );
                
                List<Record<Event>> streamingRecords = streamingFuture.get();
                resultRecords.addAll(streamingRecords);
                
                Duration latency = inputBuffer.stopLatencyWatch();
                processor.lambdaLatencyMetric.record(latency.toMillis(), TimeUnit.MILLISECONDS);
                processor.requestPayloadMetric.record(inputBuffer.getPayloadRequestSize());
                
                // Record response payload size (estimated from streaming records)
                int estimatedResponseSize = streamingRecords.size() * ESTIMATED_BYTES_PER_RECORD;
                processor.responsePayloadMetric.record(estimatedResponseSize);
                
                processor.numberOfRecordsSuccessCounter.increment(streamingRecords.size());
                processor.numberOfRequestsSuccessCounter.increment();
                processor.lambdaResponseRecordsCounter.increment(streamingRecords.size());
                
            } catch (Exception e) {
                LOG.error(NOISY, "Error processing streaming Lambda response: {}", e.getMessage(), e);
                processor.numberOfRecordsFailedCounter.increment(inputBuffer.getEventCount());
                processor.numberOfRequestsFailedCounter.increment();
                resultRecords.addAll(processor.addFailureTags(inputBuffer.getRecords()));
            }
        }
        
        return resultRecords;
    }
}
