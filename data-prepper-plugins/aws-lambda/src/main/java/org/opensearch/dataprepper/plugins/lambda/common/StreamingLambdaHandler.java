/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.common;

import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.config.StreamingOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.LambdaException;
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

    public StreamingLambdaHandler(LambdaAsyncClient lambdaAsyncClient, PluginFactory pluginFactory, InputCodec responseCodec) {
        this.lambdaAsyncClient = lambdaAsyncClient;
        this.pluginFactory = pluginFactory;
        this.responseCodec = responseCodec;
    }

    public CompletableFuture<List<Record<Event>>> invokeWithStreaming(
            String functionName,
            Buffer inputBuffer,
            StreamingOptions streamingOptions) {

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
                                responseStream.write(chunkBytes);
                                LOG.debug("Received chunk of size: {} bytes", chunkBytes.length);
                            } catch (IOException e) {
                                LOG.error("Error writing chunk to response stream", e);
                                resultFuture.completeExceptionally(e);
                            }
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
                    
                    // Handle specific Lambda exceptions like sync processing
                    if (throwable instanceof LambdaException &&
                            throwable.getMessage() != null &&
                            throwable.getMessage().contains("EXCEEDING_PAYLOAD_LIMIT_EXCEPTION")) {
                        LOG.error("Streaming Lambda exceeded payload limit", throwable);
                    }
                    
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
                
                // Add streaming metadata
                parsedEvent.put("streaming_processed", true);
                parsedEvent.put("response_size_bytes", responseBytes.length);
                
                resultRecords.add(new Record<>(parsedEvent));
            });
        }

        LOG.info("Processed streaming response: {} records from {} bytes", 
                resultRecords.size(), responseBytes.length);
        
        return resultRecords;
    }
}
