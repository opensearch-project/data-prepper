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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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
  private final String invocationType = InvocationType.REQUEST_RESPONSE.getAwsLambdaValue();
  private final String functionName = "testFunction";
  private final String batchOptionKeyName = "bathOption";
  @Mock
  private LambdaAsyncClient lambdaAsyncClient;

  public static Record<Event> getSampleRecord() {
    Event event = JacksonEvent.fromMessage(String.valueOf(UUID.randomUUID()));
    return new Record<>(event);
  }

  @Test
  void test_with_write_event_into_buffer() {
    InMemoryBuffer inMemoryBuffer = new InMemoryBuffer(batchOptionKeyName);
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

    InMemoryBuffer inMemoryBuffer = new InMemoryBuffer(batchOptionKeyName);
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

    InMemoryBuffer inMemoryBuffer = new InMemoryBuffer(batchOptionKeyName);
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

    InMemoryBuffer inMemoryBuffer = new InMemoryBuffer(batchOptionKeyName);
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

}
