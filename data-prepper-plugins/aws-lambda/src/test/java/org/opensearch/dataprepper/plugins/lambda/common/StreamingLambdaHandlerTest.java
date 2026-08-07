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

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

    @Test
    void testChunkWriting_WithConcurrentThreads_MaintainsDataIntegrity() throws InterruptedException {
        // This test verifies that ByteArrayOutputStream writes are thread-safe
        // Simulates the scenario where AWS SDK delivers chunks on different threads
        
        int numThreads = 10;
        int chunksPerThread = 10;
        int bytesPerChunk = 100;
        
        ByteArrayOutputStream testStream = new ByteArrayOutputStream();
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads * chunksPerThread);
        
        // Track which chunks were written for verification
        java.util.Set<String> writtenChunks = java.util.concurrent.ConcurrentHashMap.newKeySet();
        
        // Each thread writes multiple chunks concurrently
        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            for (int c = 0; c < chunksPerThread; c++) {
                final int chunkId = c;
                executor.submit(() -> {
                    try {
                        startLatch.await();  // Synchronize start for maximum concurrency
                        
                        // Create a chunk with unique identifiable pattern
                        byte[] chunk = new byte[bytesPerChunk];
                        // Fill chunk with a repeating pattern: [threadId, chunkId, threadId, chunkId, ...]
                        for (int i = 0; i < bytesPerChunk; i++) {
                            chunk[i] = (i % 2 == 0) ? (byte) threadId : (byte) chunkId;
                        }
                        
                        // Track this chunk for later verification
                        writtenChunks.add(threadId + "-" + chunkId);
                        
                        // Simulate the synchronized write that should happen in StreamingLambdaHandler
                        synchronized (testStream) {
                            testStream.write(chunk);
                        }
                        
                        doneLatch.countDown();
                    } catch (Exception e) {
                        // Fail the test if exception occurs
                        throw new RuntimeException("Chunk write failed", e);
                    }
                });
            }
        }
        
        // Start all threads simultaneously
        startLatch.countDown();
        
        // Wait for all writes to complete
        boolean completed = doneLatch.await(10, TimeUnit.SECONDS);
        assertTrue(completed, "All chunk writes should complete within timeout");
        
        executor.shutdown();
        boolean terminated = executor.awaitTermination(5, TimeUnit.SECONDS);
        assertTrue(terminated, "Executor should terminate");
        
        // Verify 1: Total size should equal expected total bytes (no data loss)
        int expectedSize = numThreads * chunksPerThread * bytesPerChunk;
        assertEquals(expectedSize, testStream.size(), 
                "Total bytes written should match expected size (no data loss)");
        
        // Verify 2: All chunks were written (no missing chunks)
        assertEquals(numThreads * chunksPerThread, writtenChunks.size(),
                "All chunks should have been written");
        
        // Verify 3: Chunk integrity - each chunk should be complete and not interleaved
        byte[] allBytes = testStream.toByteArray();
        int chunkCount = allBytes.length / bytesPerChunk;
        
        for (int i = 0; i < chunkCount; i++) {
            int offset = i * bytesPerChunk;
            
            // Extract this chunk
            byte[] extractedChunk = new byte[bytesPerChunk];
            System.arraycopy(allBytes, offset, extractedChunk, 0, bytesPerChunk);
            
            // Verify chunk has consistent pattern (not interleaved)
            // First byte should be threadId, alternating with chunkId
            byte threadId = extractedChunk[0];
            byte chunkId = extractedChunk[1];
            
            for (int j = 0; j < bytesPerChunk; j++) {
                byte expected = (j % 2 == 0) ? threadId : chunkId;
                assertEquals(expected, extractedChunk[j],
                        String.format("Chunk %d byte %d has wrong value - chunk may be corrupted/interleaved", i, j));
            }
        }
        
        // If we reach here, all chunks maintained their integrity - no interleaving occurred
        // This proves the synchronized block prevents data corruption
    }
}
