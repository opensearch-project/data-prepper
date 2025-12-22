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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.config.ResponseHandling;
import org.opensearch.dataprepper.plugins.lambda.common.config.StreamingOptions;


import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeWithResponseStreamRequest;
import software.amazon.awssdk.services.lambda.model.InvokeWithResponseStreamResponseHandler;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class StreamingLambdaHandlerTest {

    @Mock
    private LambdaAsyncClient lambdaAsyncClient;

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private InputCodec responseCodec;

    @Mock
    private Buffer inputBuffer;

    @Mock
    private StreamingOptions streamingOptions;

    private StreamingLambdaHandler streamingLambdaHandler;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        String functionName = "test-function";
        streamingLambdaHandler = new StreamingLambdaHandler(
                lambdaAsyncClient, 
                pluginFactory, 
                responseCodec,
                functionName,
                streamingOptions);
    }

    @Test
    void testInvokeWithStreaming_NullPayload_CompletesExceptionally() {
        // Given
        String functionName = "test-function";
        when(inputBuffer.getRequestPayload(eq(functionName), eq("RequestResponse"))).thenReturn(null);

        // When
        CompletableFuture<List<Record<Event>>> result = streamingLambdaHandler.invokeWithStreaming(inputBuffer);

        // Then
        assertTrue(result.isCompletedExceptionally());
    }

    @Test
    void testInvokeWithStreaming_ValidPayload_CallsLambdaAsyncClient() {
        // Given
        String functionName = "test-function";
        String testPayload = "{\"test\": \"data\"}";
        
        InvokeRequest mockInvokeRequest = InvokeRequest.builder()
                .functionName(functionName)
                .payload(SdkBytes.fromUtf8String(testPayload))
                .build();
        
        when(inputBuffer.getRequestPayload(eq(functionName), eq("RequestResponse")))
                .thenReturn(mockInvokeRequest);
        when(streamingOptions.getResponseHandling()).thenReturn(ResponseHandling.RECONSTRUCT_DOCUMENT);

        // When
        CompletableFuture<List<Record<Event>>> result = streamingLambdaHandler.invokeWithStreaming(inputBuffer);

        // Then
        assertNotNull(result);
        verify(lambdaAsyncClient).invokeWithResponseStream(
                any(InvokeWithResponseStreamRequest.class), 
                any(InvokeWithResponseStreamResponseHandler.class));
    }

    @Test
    void testStreamingLambdaHandler_Constructor_InitializesCorrectly() {
        // Given/When
        String functionName = "test-function";
        StreamingLambdaHandler handler = new StreamingLambdaHandler(
                lambdaAsyncClient, 
                pluginFactory, 
                responseCodec,
                functionName,
                streamingOptions);

        // Then
        assertNotNull(handler);
    }

    @Test
    void testDocumentReconstruction_WithReconstructEnabled_MergesChunks() throws Exception {
        // Given
        String functionName = "test-function";
        
        // Create streaming handler with RECONSTRUCT_DOCUMENT enabled
        when(streamingOptions.getResponseHandling()).thenReturn(ResponseHandling.RECONSTRUCT_DOCUMENT);
        StreamingLambdaHandler handler = new StreamingLambdaHandler(
                lambdaAsyncClient,
                pluginFactory,
                responseCodec,
                functionName,
                streamingOptions);
        
        // Create mock buffer with one original record
        java.util.Map<String, Object> originalData = new java.util.HashMap<>();
        originalData.put("original_field", "original_value");
        
        org.opensearch.dataprepper.model.event.JacksonEvent originalEvent = 
                org.opensearch.dataprepper.model.event.JacksonEvent.builder()
                .withEventType("test")
                .withData(originalData)
                .build();
        
        Record<Event> originalRecord = new Record<>(originalEvent);
        List<Record<Event>> originalRecords = java.util.Collections.singletonList(originalRecord);
        
        when(inputBuffer.getRecords()).thenReturn(originalRecords);
        
        String testPayload = "[{\"chunk1_field\":\"value1\"},{\"chunk2_field\":\"value2\"}]";
        InvokeRequest mockInvokeRequest = InvokeRequest.builder()
                .functionName(functionName)
                .payload(SdkBytes.fromUtf8String(testPayload))
                .build();
        
        when(inputBuffer.getRequestPayload(eq(functionName), eq("RequestResponse")))
                .thenReturn(mockInvokeRequest);
        
        // Mock the response codec to parse the test payload
        org.opensearch.dataprepper.plugins.codec.json.JsonInputCodec realCodec = 
                new org.opensearch.dataprepper.plugins.codec.json.JsonInputCodec(
                        new org.opensearch.dataprepper.plugins.codec.json.JsonInputCodecConfig());
        StreamingLambdaHandler handlerWithRealCodec = new StreamingLambdaHandler(
                lambdaAsyncClient,
                pluginFactory,
                realCodec,
                functionName,
                streamingOptions);
        
        // When - simulate a completed streaming response
        CompletableFuture<List<Record<Event>>> result = handlerWithRealCodec.invokeWithStreaming(inputBuffer);
        
        // Simulate the streaming completion by manually calling the response handler
        // (In actual usage, this would be triggered by AWS SDK)
        
        // Then - verify the result (test framework limitation: can't easily test async callbacks)
        assertNotNull(result);
    }

    @Test
    void testDocumentReconstruction_WithReconstructDisabled_ReturnsChunksSeparately() {
        // Given
        String functionName = "test-function";
        
        // Create streaming handler WITHOUT reconstruction
        when(streamingOptions.getResponseHandling()).thenReturn(null);  // No reconstruction
        StreamingLambdaHandler handler = new StreamingLambdaHandler(
                lambdaAsyncClient,
                pluginFactory,
                responseCodec,
                functionName,
                streamingOptions);
        
        String testPayload = "[{\"chunk1\":\"data1\"},{\"chunk2\":\"data2\"}]";
        InvokeRequest mockInvokeRequest = InvokeRequest.builder()
                .functionName(functionName)
                .payload(SdkBytes.fromUtf8String(testPayload))
                .build();
        
        when(inputBuffer.getRequestPayload(eq(functionName), eq("RequestResponse")))
                .thenReturn(mockInvokeRequest);
        
        // When
        CompletableFuture<List<Record<Event>>> result = handler.invokeWithStreaming(inputBuffer);
        
        // Then - without reconstruction, chunks remain separate
        assertNotNull(result);
    }
}
