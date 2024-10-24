/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.common.accumulator;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import static org.mockito.ArgumentMatchers.any;
import org.mockito.Mock;
import static org.mockito.Mockito.when;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.InMemoryBuffer;
import org.opensearch.dataprepper.plugins.lambda.common.config.InvocationType;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

@ExtendWith(MockitoExtension.class)
class InMemoryBufferTest {

    public static final int MAX_EVENTS = 55;
    @Mock
    private LambdaAsyncClient lambdaAsyncClient;

    private final InvocationType invocationType = InvocationType.REQUEST_RESPONSE;

    private final String functionName = "testFunction";

    private InMemoryBuffer inMemoryBuffer;

    @Test
    void test_with_write_event_into_buffer() throws IOException {
        inMemoryBuffer = new InMemoryBuffer(lambdaAsyncClient, functionName, invocationType);

        while (inMemoryBuffer.getEventCount() < MAX_EVENTS) {
            OutputStream outputStream = inMemoryBuffer.getOutputStream();
            outputStream.write(generateByteArray());
            int eventCount = inMemoryBuffer.getEventCount() +1;
            inMemoryBuffer.setEventCount(eventCount);
        }
        assertThat(inMemoryBuffer.getSize(), greaterThanOrEqualTo(54110L));
        assertThat(inMemoryBuffer.getEventCount(), equalTo(MAX_EVENTS));
        assertThat(inMemoryBuffer.getDuration(), notNullValue());
        assertThat(inMemoryBuffer.getDuration(), greaterThanOrEqualTo(Duration.ZERO));
    }

    @Test
    void test_with_write_event_into_buffer_and_flush_toLambda() throws IOException {

        // Mock the response of the invoke method
        InvokeResponse mockResponse = InvokeResponse.builder()
                .statusCode(200) // HTTP 200 for successful invocation
                .payload(SdkBytes.fromString("{\"key\": \"value\"}", java.nio.charset.StandardCharsets.UTF_8))
                .build();
        CompletableFuture<InvokeResponse> future = CompletableFuture.completedFuture(mockResponse);
        when(lambdaAsyncClient.invoke(any(InvokeRequest.class))).thenReturn(future);

        inMemoryBuffer = new InMemoryBuffer(lambdaAsyncClient, functionName, invocationType);
        while (inMemoryBuffer.getEventCount() < MAX_EVENTS) {
            OutputStream outputStream = inMemoryBuffer.getOutputStream();
            outputStream.write(generateByteArray());
            int eventCount = inMemoryBuffer.getEventCount() +1;
            inMemoryBuffer.setEventCount(eventCount);
        }
        assertDoesNotThrow(() -> {
            CompletableFuture<InvokeResponse> responseFuture = inMemoryBuffer.flushToLambda(invocationType);
            InvokeResponse response = responseFuture.join();
            assertThat(response.statusCode(), equalTo(200));
        });
    }

    @Test
    void test_uploadedToLambda_success() throws IOException {
        // Mock the response of the invoke method
        InvokeResponse mockResponse = InvokeResponse.builder()
                .statusCode(200) // HTTP 200 for successful invocation
                .payload(SdkBytes.fromString("{\"key\": \"value\"}", java.nio.charset.StandardCharsets.UTF_8))
                .build();

        CompletableFuture<InvokeResponse> future = CompletableFuture.completedFuture(mockResponse);
        when(lambdaAsyncClient.invoke(any(InvokeRequest.class))).thenReturn(future);


        inMemoryBuffer = new InMemoryBuffer(lambdaAsyncClient, functionName, invocationType);
        assertNotNull(inMemoryBuffer);
        OutputStream outputStream = inMemoryBuffer.getOutputStream();
        outputStream.write(generateByteArray());
        inMemoryBuffer.setEventCount(1);

        assertDoesNotThrow(() -> {
            CompletableFuture<InvokeResponse> responseFuture = inMemoryBuffer.flushToLambda(invocationType);
            InvokeResponse response = responseFuture.join();
            assertThat(response.statusCode(), equalTo(200));
        });
    }

    @Test
    void test_uploadedToLambda_fails() {
        // Mock an exception when invoking lambda
        SdkClientException sdkClientException = SdkClientException.create("Mock exception");

        CompletableFuture<InvokeResponse> future = new CompletableFuture<>();
        future.completeExceptionally(sdkClientException);

        when(lambdaAsyncClient.invoke(any(InvokeRequest.class))).thenReturn(future);

        inMemoryBuffer = new InMemoryBuffer(lambdaAsyncClient, functionName, invocationType);
        assertNotNull(inMemoryBuffer);

        // Execute and assert exception
        CompletionException exception = assertThrows(CompletionException.class, () -> {
            CompletableFuture<InvokeResponse> responseFuture = inMemoryBuffer.flushToLambda(invocationType);
            responseFuture.join(); // This will throw CompletionException
        });

        // Verify that the cause of the CompletionException is the SdkClientException we threw
        assertThat(exception.getCause(), instanceOf(SdkClientException.class));
        assertThat(exception.getCause().getMessage(), equalTo("Mock exception"));

    }

    private byte[] generateByteArray() {
        byte[] bytes = new byte[1000];
        for (int i = 0; i < 1000; i++) {
            bytes[i] = (byte) i;
        }
        return bytes;
    }
}