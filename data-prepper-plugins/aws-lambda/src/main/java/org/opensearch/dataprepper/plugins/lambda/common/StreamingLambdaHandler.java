/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.lambda.common;

import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.config.ResponseHandling;
import org.opensearch.dataprepper.plugins.lambda.common.config.StreamingOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeWithResponseStreamRequest;
import software.amazon.awssdk.services.lambda.model.InvokeWithResponseStreamResponseHandler;
import software.amazon.awssdk.services.lambda.model.ResponseStreamingInvocationType;
import software.amazon.awssdk.services.lambda.model.invokewithresponsestreamresponseevent.DefaultPayloadChunk;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Handles actual streaming Lambda invocations using AWS SDK streaming API
 */
public class StreamingLambdaHandler {
    private static final Logger LOG = LoggerFactory.getLogger(StreamingLambdaHandler.class);

    private final LambdaAsyncClient lambdaAsyncClient;
    private final PluginFactory pluginFactory;
    private final InputCodec responseCodec;
    private final String functionName;
    private final StreamingOptions streamingOptions;

    public StreamingLambdaHandler(
            LambdaAsyncClient lambdaAsyncClient, 
            PluginFactory pluginFactory, 
            InputCodec responseCodec,
            String functionName,
            StreamingOptions streamingOptions) {
        this.lambdaAsyncClient = lambdaAsyncClient;
        this.pluginFactory = pluginFactory;
        this.responseCodec = responseCodec;
        this.functionName = functionName;
        this.streamingOptions = streamingOptions;
    }

    public CompletableFuture<List<Record<Event>>> invokeWithStreaming(Buffer inputBuffer) {

        CompletableFuture<List<Record<Event>>> resultFuture = new CompletableFuture<>();
        ByteArrayOutputStream responseStream = new ByteArrayOutputStream();

        // Get the InvokeRequest from buffer and extract payload
        InvokeRequest invokeRequest = inputBuffer.getRequestPayload(functionName, "RequestResponse");
        if (invokeRequest == null) {
            resultFuture.completeExceptionally(new IllegalArgumentException("No payload in buffer"));
            return resultFuture;
        }

        InvokeWithResponseStreamRequest request = InvokeWithResponseStreamRequest.builder()
                .functionName(functionName)
                .invocationType(ResponseStreamingInvocationType.REQUEST_RESPONSE)
                .payload(invokeRequest.payload())
                .build();

        InvokeWithResponseStreamResponseHandler responseHandler = InvokeWithResponseStreamResponseHandler.builder()
                .onResponse(response -> {
                    LOG.debug("Streaming response started for function: {}", functionName);
                })
                .onEventStream(publisher -> {
                    publisher.subscribe(event -> {
                        if (event instanceof DefaultPayloadChunk) {
                            DefaultPayloadChunk chunk = (DefaultPayloadChunk) event;
                            try {
                                // DefaultPayloadChunk should have payload() method
                                byte[] chunkBytes = chunk.payload().asByteArray();
                                // Synchronize access to ByteArrayOutputStream as it's not thread-safe
                                // AWS SDK may deliver chunks on different threads
                                synchronized (responseStream) {
                                    responseStream.write(chunkBytes);
                                }
                                LOG.debug("Received chunk of size: {} bytes", chunkBytes.length);
                            } catch (IOException e) {
                                LOG.error("Error writing chunk to response stream", e);
                                resultFuture.completeExceptionally(e);
                            }
                        } else {
                            // Other events (e.g., InvokeComplete) are handled by onComplete()
                            LOG.debug("Ignoring non-payload Lambda stream event: {}", event.getClass().getSimpleName());
                        }
                    });
                })
                .onComplete(() -> {
                    try {
                        byte[] completeResponse = responseStream.toByteArray();
                        LOG.debug("Streaming response complete. Total size: {} bytes", completeResponse.length);
                        
                        List<Record<Event>> processedRecords = processStreamingResponse(
                                completeResponse, inputBuffer, streamingOptions);
                        resultFuture.complete(processedRecords);
                        
                    } catch (Exception e) {
                        LOG.error("Error processing complete streaming response", e);
                        resultFuture.completeExceptionally(e);
                    }
                })
                .onError(throwable -> {
                    LOG.error("Error in streaming Lambda invocation", throwable);
                    resultFuture.completeExceptionally(throwable);
                })
                .build();

        lambdaAsyncClient.invokeWithResponseStream(request, responseHandler);
        return resultFuture;
    }

    private List<Record<Event>> processStreamingResponse(
            byte[] responseBytes,
            Buffer inputBuffer,
            StreamingOptions streamingOptions) throws IOException {

        List<Record<Event>> resultRecords = new ArrayList<>();

        try (ByteArrayInputStream responseStream = new ByteArrayInputStream(responseBytes)) {
            responseCodec.parse(responseStream, record -> {
                Event parsedEvent = record.getData();
                resultRecords.add(new Record<>(parsedEvent));
            });
        }

        LOG.info("Processed streaming response: {} records from {} bytes", 
                resultRecords.size(), responseBytes.length);
        
        // Apply response handling strategy
        return applyResponseHandling(resultRecords, inputBuffer, streamingOptions);
    }

    /**
     * Applies the configured response handling strategy to the parsed records.
     * 
     * @param parsedRecords Records parsed from the streaming response
     * @param inputBuffer Original input buffer containing source events
     * @param streamingOptions Configuration for response handling
     * @return Processed records based on the handling strategy
     */
    private List<Record<Event>> applyResponseHandling(
            List<Record<Event>> parsedRecords,
            Buffer inputBuffer,
            StreamingOptions streamingOptions) {
        
        if (streamingOptions == null || 
            streamingOptions.getResponseHandling() != ResponseHandling.RECONSTRUCT_DOCUMENT) {
            // No reconstruction - return records as-is
            return parsedRecords;
        }

        // Reconstruct: merge all chunks into a single document
        return reconstructDocument(parsedRecords, inputBuffer);
    }

    /**
     * Reconstructs a single document from multiple streaming chunks.
     * All chunks from the streaming response are merged into one Event,
     * matching the original input event count.
     * 
     * <p>The reconstructed event retains the original event's EventHandle, enabling proper end-to-end
     * acknowledgement tracking. Chunks are treated as transport-level fragments (due to Lambda's 
     * response size limits) and are not tracked separately in the acknowledgement system - only the 
     * final reconstructed event is acknowledged downstream.</p>
     * 
     * @param parsedRecords All chunks parsed from the streaming response
     * @param inputBuffer Original input buffer
     * @return List containing the reconstructed document(s)
     */
    private List<Record<Event>> reconstructDocument(
            List<Record<Event>> parsedRecords,
            Buffer inputBuffer) {
        
        if (parsedRecords.isEmpty()) {
            return parsedRecords;
        }

        // Get the original records to maintain event handle relationships
        List<Record<Event>> originalRecords = inputBuffer.getRecords();
        
        if (originalRecords.isEmpty()) {
            LOG.warn("No original records found in buffer for reconstruction");
            return parsedRecords;
        }

        // Defensive check: reconstruct-document requires exactly 1 event per buffer
        // This should be enforced by validation in LambdaProcessor, but we check here to prevent silent failures
        if (originalRecords.size() != 1) {
            String errorMsg = String.format(
                "reconstruct-document mode requires exactly 1 event per buffer, found %d events. " +
                "This should have been prevented by configuration validation. " +
                "Please ensure batch.threshold.event_count is set to 1.",
                originalRecords.size()
            );
            LOG.error(errorMsg);
            throw new IllegalStateException(errorMsg);
        }

        // Merge all chunks into the single original record
        // This handles: 1 input event → multiple chunks → 1 reconstructed event
        Event reconstructedEvent = originalRecords.get(0).getData();
        
        // Merge all parsed chunks into the reconstructed event
        for (Record<Event> parsedRecord : parsedRecords) {
            Event chunkEvent = parsedRecord.getData();
            reconstructedEvent.merge(chunkEvent);
        }

        LOG.info("Reconstructed {} chunks into {} document(s)", 
                parsedRecords.size(), originalRecords.size());
        
        // Return the original records (now containing merged data)
        return originalRecords;
    }
}
