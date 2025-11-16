/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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
        streamingLambdaHandler = new StreamingLambdaHandler(lambdaAsyncClient, pluginFactory, responseCodec);
    }

    @Test
    void testInvokeWithStreaming_NullPayload_CompletesExceptionally() {
        // Given
        String functionName = "test-function";
        when(inputBuffer.getRequestPayload(eq(functionName), eq("RequestResponse"))).thenReturn(null);

        // When
        CompletableFuture<List<Record<Event>>> result = streamingLambdaHandler.invokeWithStreaming(
                functionName, inputBuffer, streamingOptions);

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
        when(streamingOptions.getResponseHandling()).thenReturn("reconstruct_document");

        // When
        CompletableFuture<List<Record<Event>>> result = streamingLambdaHandler.invokeWithStreaming(
                functionName, inputBuffer, streamingOptions);

        // Then
        assertNotNull(result);
        verify(lambdaAsyncClient).invokeWithResponseStream(
                any(InvokeWithResponseStreamRequest.class), 
                any(InvokeWithResponseStreamResponseHandler.class));
    }

    @Test
    void testStreamingLambdaHandler_Constructor_InitializesCorrectly() {
        // Given/When
        StreamingLambdaHandler handler = new StreamingLambdaHandler(lambdaAsyncClient, pluginFactory, responseCodec);

        // Then
        assertNotNull(handler);
    }
}
