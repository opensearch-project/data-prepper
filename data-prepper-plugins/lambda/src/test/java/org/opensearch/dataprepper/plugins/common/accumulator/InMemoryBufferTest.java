/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.common.accumulator;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import org.hamcrest.Matchers;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import static org.mockito.ArgumentMatchers.any;
import org.mockito.Mock;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.common.lambda.accumlator.InMemoryBuffer;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

@ExtendWith(MockitoExtension.class)
class InMemoryBufferTest {

    public static final int MAX_EVENTS = 55;
    @Mock
    private LambdaClient lambdaClient;

    private final String functionName = "testFunction";

    private final String invocationType = "Event";

    private InMemoryBuffer inMemoryBuffer;

    @Test
    void test_with_write_event_into_buffer() throws IOException {
        inMemoryBuffer = new InMemoryBuffer(lambdaClient, functionName, invocationType);

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
    @Disabled("unstable")
    /**
     * There are 5 checkpoints in the tests as below
     *     |-----------upperBoundDuration-------------|
     * startTime --- stopWatchStart --- endTime --- checkpoint --- stopwatchGetDuration
     *                                 |-lowerBoundDuration-|
     *                       |------------inMemoryBuffer.Duration-------------|
     *  This test assumes the startTime and stopWatchStart are same, checkpoint and stopwatchGetDuration are same.
     *  However, they are not true at some systems.
     */
    void getDuration_provides_duration_within_expected_range() throws IOException, InterruptedException {
        Instant startTime = Instant.now();
        inMemoryBuffer = new InMemoryBuffer(lambdaClient, functionName, invocationType);
        Instant endTime = Instant.now();


        while (inMemoryBuffer.getEventCount() < MAX_EVENTS) {
            OutputStream outputStream = inMemoryBuffer.getOutputStream();
            outputStream.write(generateByteArray());
            int eventCount = inMemoryBuffer.getEventCount() +1;
            inMemoryBuffer.setEventCount(eventCount);
        }
        Thread.sleep(100);

        Instant durationCheckpointTime = Instant.now();
        Duration duration = inMemoryBuffer.getDuration();
        assertThat(duration, notNullValue());

        Duration upperBoundDuration = Duration.between(startTime, durationCheckpointTime).truncatedTo(ChronoUnit.MILLIS);
        Duration lowerBoundDuration = Duration.between(endTime, durationCheckpointTime).truncatedTo(ChronoUnit.MILLIS);
        assertThat(duration, greaterThanOrEqualTo(lowerBoundDuration));
        assertThat(duration, lessThanOrEqualTo(upperBoundDuration));
    }

    @Test
    void test_with_write_event_into_buffer_and_flush_toLambda() throws IOException {

        // Mock the response of the invoke method
        InvokeResponse mockResponse = InvokeResponse.builder()
                .statusCode(200) // HTTP 200 for successful invocation
                .payload(SdkBytes.fromString("{\"key\": \"value\"}", java.nio.charset.StandardCharsets.UTF_8))
                .build();
        when(lambdaClient.invoke(any(InvokeRequest.class))).thenReturn(mockResponse);

        inMemoryBuffer = new InMemoryBuffer(lambdaClient, functionName, invocationType);
        while (inMemoryBuffer.getEventCount() < MAX_EVENTS) {
            OutputStream outputStream = inMemoryBuffer.getOutputStream();
            outputStream.write(generateByteArray());
            int eventCount = inMemoryBuffer.getEventCount() +1;
            inMemoryBuffer.setEventCount(eventCount);
        }
        assertDoesNotThrow(() -> {
            inMemoryBuffer.flushToLambdaAsync();
        });
    }

    @Test
    void test_uploadedToLambda_success() throws IOException {
        // Mock the response of the invoke method
        InvokeResponse mockResponse = InvokeResponse.builder()
                .statusCode(200) // HTTP 200 for successful invocation
                .payload(SdkBytes.fromString("{\"key\": \"value\"}", java.nio.charset.StandardCharsets.UTF_8))
                .build();
        when(lambdaClient.invoke(any(InvokeRequest.class))).thenReturn(mockResponse);
        inMemoryBuffer = new InMemoryBuffer(lambdaClient, functionName, invocationType);
        Assertions.assertNotNull(inMemoryBuffer);
        OutputStream outputStream = inMemoryBuffer.getOutputStream();
        outputStream.write(generateByteArray());
        assertDoesNotThrow(() -> {
            inMemoryBuffer.flushToLambdaAsync();
        });
    }

    @Test
    void test_uploadedToLambda_fails() {
        // Mock the response of the invoke method
        InvokeResponse mockResponse = InvokeResponse.builder()
                .statusCode(200) // HTTP 200 for successful invocation
                .payload(SdkBytes.fromString("{\"key\": \"value\"}", java.nio.charset.StandardCharsets.UTF_8))
                .build();
        SdkClientException sdkClientException = mock(SdkClientException.class);
        when(lambdaClient.invoke(any(InvokeRequest.class)))
                .thenThrow(sdkClientException);
        inMemoryBuffer = new InMemoryBuffer(lambdaClient, functionName, invocationType);

        Assertions.assertNotNull(inMemoryBuffer);
        SdkClientException actualException = assertThrows(SdkClientException.class, () -> inMemoryBuffer.flushToLambdaAsync());
        assertThat(actualException, Matchers.equalTo(sdkClientException));
    }

    private byte[] generateByteArray() {
        byte[] bytes = new byte[1000];
        for (int i = 0; i < 1000; i++) {
            bytes[i] = (byte) i;
        }
        return bytes;
    }
}