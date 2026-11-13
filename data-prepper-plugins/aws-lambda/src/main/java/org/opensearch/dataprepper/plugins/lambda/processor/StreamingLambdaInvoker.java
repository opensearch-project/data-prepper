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
import org.opensearch.dataprepper.plugins.lambda.common.StreamingLambdaInvocationException;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.LambdaException;
import software.amazon.awssdk.services.lambda.model.ResourceNotFoundException;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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
        // Build the request buffers only. The streaming call below is the only invocation.
        List<Buffer> buffers;
        try {
            buffers = LambdaCommonHandler.createBuffers(
                    recordsToLambda,
                    processor.lambdaProcessorConfig,
                    new OutputCodecContext());
        } catch (Exception e) {
            LOG.error(NOISY, "Error creating buffers for streaming", e);
            processor.numberOfRecordsFailedCounter.increment(recordsToLambda.size());
            processor.numberOfRequestsFailedCounter.increment();
            resultRecords.addAll(processor.addFailureTags(recordsToLambda));
            return resultRecords;
        }
        
        // Process each buffer with streaming
        for (Buffer inputBuffer : buffers) {

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
                
            } catch (final ExecutionException e) {
                failBuffer(e.getCause() != null ? e.getCause() : e, inputBuffer, resultRecords);
            } catch (final InterruptedException e) {
                // Keep the interrupt so the pipeline can shut down
                Thread.currentThread().interrupt();
                failBuffer(e, inputBuffer, resultRecords);
            } catch (final Exception e) {
                failBuffer(e, inputBuffer, resultRecords);
            }
        }
        
        return resultRecords;
    }

    /**
     * Logs why a buffer's invocation failed, counts it and returns its records tagged as failed. The streaming call
     * reports failures through its future, so the cause arrives wrapped in an ExecutionException.
     */
    private void failBuffer(final Throwable failure, final Buffer inputBuffer, final List<Record<Event>> resultRecords) {
        if (failure instanceof StreamingLambdaInvocationException) {
            // Expected exception: the function failed, for example by timing out
            LOG.error(NOISY, "Lambda function error during streaming invocation: {}", failure.getMessage());
        } else if (failure instanceof ResourceNotFoundException) {
            // Expected exception: Lambda function not found
            LOG.error(NOISY, "Lambda function not found: {}", failure.getMessage());
        } else if (failure instanceof LambdaException) {
            // Expected exception: Access denied, invalid parameters, throttling, etc.
            LOG.error(NOISY, "Lambda invocation failed: {}", failure.getMessage());
        } else if (failure instanceof AwsServiceException) {
            // Expected exception: General AWS service errors (credentials, permissions, etc.)
            LOG.error(NOISY, "AWS service error during Lambda invocation: {}", failure.getMessage());
        } else {
            // Unexpected exception: Coding issues, null pointers, parsing errors, etc.
            LOG.error(NOISY, "Unexpected error processing streaming Lambda response.", failure);
        }
        processor.numberOfRecordsFailedCounter.increment(inputBuffer.getEventCount());
        processor.numberOfRequestsFailedCounter.increment();
        resultRecords.addAll(processor.addFailureTags(inputBuffer.getRecords()));
    }
}
