/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.lambda.common.accumulator;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.InMemoryBuffer;
import org.opensearch.dataprepper.plugins.lambda.common.config.InvocationType;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

@ExtendWith(MockitoExtension.class)
class InMemoryBufferTest {

    public static final int MAX_EVENTS = 55;
    private String invocationType;
    private String functionName;
    private final String batchOptionKeyName = "bathOption";
    @Mock
    private LambdaAsyncClient lambdaAsyncClient;

    public static Record<Event> getSampleRecord() {
        Event event = JacksonEvent.fromMessage(String.valueOf(UUID.randomUUID()));
        return new Record<>(event);
    }

    @BeforeEach
    void setUp() {
        invocationType = InvocationType.REQUEST_RESPONSE.getAwsLambdaValue();
        functionName = UUID.randomUUID().toString();
    }

    private InMemoryBuffer createObjectUnderTest() {
        return new InMemoryBuffer(batchOptionKeyName);
    }

    @Test
    void test_with_write_event_into_buffer() {
        InMemoryBuffer inMemoryBuffer = createObjectUnderTest();
        //UUID based random event created. Each UUID string is of 36 characters long
        int eachEventSize = 36;
        long sizeToAssert = eachEventSize * MAX_EVENTS;
        while (inMemoryBuffer.getEventCount() < MAX_EVENTS) {
            inMemoryBuffer.addRecord(getSampleRecord());
        }
        assertThat(inMemoryBuffer.getSize(), greaterThanOrEqualTo(sizeToAssert));
        assertThat(inMemoryBuffer.getEventCount(), equalTo(MAX_EVENTS));
        assertThat(inMemoryBuffer.getDuration(), notNullValue());
        assertThat(inMemoryBuffer.getDuration(), greaterThanOrEqualTo(Duration.ZERO));
    }

    @Test
    void test_with_write_event_into_buffer_and_flush_toLambda() {

        // Mock the response of the invoke method
        InvokeResponse mockResponse = InvokeResponse.builder()
                .statusCode(200) // HTTP 200 for successful invocation
                .payload(
                        SdkBytes.fromString("{\"key\": \"value\"}", java.nio.charset.StandardCharsets.UTF_8))
                .build();
        CompletableFuture<InvokeResponse> future = CompletableFuture.completedFuture(mockResponse);
        when(lambdaAsyncClient.invoke(any(InvokeRequest.class))).thenReturn(future);

        InMemoryBuffer inMemoryBuffer = createObjectUnderTest();
        while (inMemoryBuffer.getEventCount() < MAX_EVENTS) {
            inMemoryBuffer.addRecord(getSampleRecord());
        }
        assertDoesNotThrow(() -> {
            InvokeRequest requestPayload = inMemoryBuffer.getRequestPayload(
                    functionName, invocationType);
            CompletableFuture<InvokeResponse> responseFuture = lambdaAsyncClient.invoke(requestPayload);
            InvokeResponse response = responseFuture.join();
            assertThat(response.statusCode(), equalTo(200));
        });
    }

    @Test
    void test_uploadedToLambda_success() {
        // Mock the response of the invoke method
        InvokeResponse mockResponse = InvokeResponse.builder()
                .statusCode(200) // HTTP 200 for successful invocation
                .payload(
                        SdkBytes.fromString("{\"key\": \"value\"}", java.nio.charset.StandardCharsets.UTF_8))
                .build();

        CompletableFuture<InvokeResponse> future = CompletableFuture.completedFuture(mockResponse);
        when(lambdaAsyncClient.invoke(any(InvokeRequest.class))).thenReturn(future);

        InMemoryBuffer inMemoryBuffer = createObjectUnderTest();
        assertNotNull(inMemoryBuffer);
        inMemoryBuffer.addRecord(getSampleRecord());

        assertDoesNotThrow(() -> {
            InvokeRequest requestPayload = inMemoryBuffer.getRequestPayload(
                    functionName, invocationType);
            CompletableFuture<InvokeResponse> responseFuture = lambdaAsyncClient.invoke(requestPayload);
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

        InMemoryBuffer inMemoryBuffer = createObjectUnderTest();
        assertNotNull(inMemoryBuffer);

        assertNull(inMemoryBuffer.getRequestPayload(functionName, invocationType));
        inMemoryBuffer.addRecord(getSampleRecord());
        // Execute and assert exception
        CompletionException exception = assertThrows(CompletionException.class, () -> {
            InvokeRequest requestPayload = inMemoryBuffer.getRequestPayload(
                    functionName, invocationType);
            CompletableFuture<InvokeResponse> responseFuture = lambdaAsyncClient.invoke(requestPayload);
            responseFuture.join();// This will throw CompletionException
        });

        // Verify that the cause of the CompletionException is the SdkClientException we threw
        assertThat(exception.getCause(), instanceOf(SdkClientException.class));
        assertThat(exception.getCause().getMessage(), equalTo("Mock exception"));
    }

    @Test
    void getPayloadRequestSize_returns_0_before_complete() {
        assertThat(createObjectUnderTest().getPayloadRequestSize(), equalTo(0L));
    }

    @Test
    void getPayloadRequestSize_returns_size_of_bytes_after_getRequestPayload() {
        final InMemoryBuffer objectUnderTest = createObjectUnderTest();
        IntStream.range(0, MAX_EVENTS)
                .forEach(i -> objectUnderTest.addRecord(getSampleRecord()));

        objectUnderTest.getRequestPayload(functionName, invocationType);
        assertAll(
                () -> assertThat(objectUnderTest.getPayloadRequestSize(), greaterThanOrEqualTo(2800L)),
                () -> assertThat(objectUnderTest.getPayloadRequestSize(), lessThanOrEqualTo(2900L))
        );
    }

    @ParameterizedTest
    @EnumSource(InvocationType.class)
    void getRequestPayload_returns_correct_InvokeRequest(final InvocationType invocationTypeEnum) throws IOException {
        invocationType = invocationTypeEnum.getAwsLambdaValue();
        final InMemoryBuffer objectUnderTest = createObjectUnderTest();

        final List<Record<Event>> sampleRecords = IntStream.range(0, MAX_EVENTS)
                .mapToObj(i -> getSampleRecord())
                .collect(Collectors.toList());

        sampleRecords.stream()
                .forEach(objectUnderTest::addRecord);

        final InvokeRequest invokeRequest = objectUnderTest.getRequestPayload(functionName, invocationType);

        assertAll(
                () -> assertThat(invokeRequest.functionName(), equalTo(functionName)),
                () -> assertThat(invokeRequest.invocationTypeAsString(), equalTo(invocationType))
        );

        final SdkBytes payload = invokeRequest.payload();
        assertThat(payload, notNullValue());

        final ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> actualMap = objectMapper.readValue(payload.asInputStream(), Map.class);
        assertThat(actualMap, notNullValue());
        assertThat(actualMap, hasKey(batchOptionKeyName));
        assertThat(actualMap.get(batchOptionKeyName), instanceOf(List.class));
        final List<Map<String, Object>> actualList = (List<Map<String, Object>>) actualMap.get(batchOptionKeyName);
        assertThat(actualList.size(), equalTo(MAX_EVENTS));
        for (int i = 0; i < MAX_EVENTS; i++) {
            final Map<String, Object> eventObject = actualList.get(i);
            assertThat(eventObject, notNullValue());
            assertThat(eventObject, instanceOf(Map.class));
            assertThat(eventObject, hasKey("message"));
            assertThat(eventObject.get("message"), instanceOf(String.class));

            final Record<Event> eventRecord = sampleRecords.get(i);
            assertThat(eventObject, equalTo(eventRecord.getData().toMap()));
        }
    }
}
