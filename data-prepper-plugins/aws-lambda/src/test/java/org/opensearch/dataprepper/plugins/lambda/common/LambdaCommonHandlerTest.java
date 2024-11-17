package org.opensearch.dataprepper.plugins.lambda.common;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.config.BatchOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.InvocationType;
import org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig;
import org.opensearch.dataprepper.plugins.lambda.common.config.ThresholdOptions;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import static org.mockito.ArgumentMatchers.any;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class LambdaCommonHandlerTest {

  @Mock
  private LambdaAsyncClient lambdaAsyncClient;

  @Mock
  private LambdaCommonConfig config;

  @Mock
  private BatchOptions batchOptions;

  @Mock
  private OutputCodecContext outputCodecContext;

  @Test
  void testCheckStatusCode() {
    InvokeResponse successResponse = InvokeResponse.builder().statusCode(200).build();
    InvokeResponse failureResponse = InvokeResponse.builder().statusCode(400).build();

    assertTrue(LambdaCommonHandler.checkStatusCode(successResponse));
    assertFalse(LambdaCommonHandler.checkStatusCode(failureResponse));
  }

  @Test
  void testWaitForFutures() {
    List<CompletableFuture<InvokeResponse>> futureList = new ArrayList<>();
    CompletableFuture<InvokeResponse> future1 = new CompletableFuture<>();
    CompletableFuture<InvokeResponse> future2 = new CompletableFuture<>();
    futureList.add(future1);
    futureList.add(future2);

    // Simulate completion of futures
    future1.complete(InvokeResponse.builder().build());
    future2.complete(InvokeResponse.builder().build());

    LambdaCommonHandler.waitForFutures(futureList);

    assertTrue(futureList.isEmpty());
  }

  @Test
  void testSendRecords() {
    when(config.getBatchOptions()).thenReturn(batchOptions);
    when(batchOptions.getThresholdOptions()).thenReturn(mock(ThresholdOptions.class));
    when(batchOptions.getKeyName()).thenReturn("testKey");
    when(config.getFunctionName()).thenReturn("testFunction");
    when(config.getInvocationType()).thenReturn(InvocationType.REQUEST_RESPONSE);
    when(lambdaAsyncClient.invoke(any(InvokeRequest.class)))
            .thenReturn(CompletableFuture.completedFuture(InvokeResponse.builder().statusCode(200).build()));

    Event mockEvent = mock(Event.class);
    when(mockEvent.toMap()).thenReturn(Collections.singletonMap("testKey", "testValue"));
    List<Record<Event>> records = Collections.singletonList(new Record<>(mockEvent));

    BiFunction<Buffer, InvokeResponse, List<Record<Event>>> successHandler = (buffer, response) -> new ArrayList<>();
    BiConsumer<Buffer, List<Record<Event>>> failureHandler = (buffer, resultRecords) -> {};

    List<Record<Event>> result = LambdaCommonHandler.sendRecords(records, config, lambdaAsyncClient, outputCodecContext, successHandler, failureHandler);

    assertNotNull(result);
    verify(lambdaAsyncClient, atLeastOnce()).invoke(any(InvokeRequest.class));
  }

  @Test
  void testSendRecordsWithNullKeyName() {
    when(config.getBatchOptions()).thenReturn(batchOptions);
    when(batchOptions.getThresholdOptions()).thenReturn(mock(ThresholdOptions.class));
    when(batchOptions.getKeyName()).thenReturn(null);
    when(config.getInvocationType()).thenReturn(InvocationType.REQUEST_RESPONSE);
    when(config.getFunctionName()).thenReturn("testFunction");

    Event mockEvent = mock(Event.class);
    when(mockEvent.toMap()).thenReturn(Collections.singletonMap("testKey", "testValue"));
    List<Record<Event>> records = Collections.singletonList(new Record<>(mockEvent));

    BiFunction<Buffer, InvokeResponse, List<Record<Event>>> successHandler = (buffer, response) -> new ArrayList<>();
    BiConsumer<Buffer, List<Record<Event>>> failureHandler = (buffer, resultRecords) -> {};

    assertThrows(NullPointerException.class, () ->
            LambdaCommonHandler.sendRecords(records, config, lambdaAsyncClient, outputCodecContext, successHandler, failureHandler)
    );
  }

  @Test
  void testSendRecordsWithFailure() {
    when(config.getBatchOptions()).thenReturn(batchOptions);
    when(batchOptions.getThresholdOptions()).thenReturn(mock(ThresholdOptions.class));
    when(batchOptions.getKeyName()).thenReturn("testKey");
    when(config.getFunctionName()).thenReturn("testFunction");
    when(config.getInvocationType()).thenReturn(InvocationType.REQUEST_RESPONSE);
    when(lambdaAsyncClient.invoke(any(InvokeRequest.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Test exception")));

    List<Record<Event>> records = new ArrayList<>();
    records.add(new Record<>(mock(Event.class)));

    BiFunction<Buffer, InvokeResponse, List<Record<Event>>> successHandler = (buffer, response) -> new ArrayList<>();
    BiConsumer<Buffer, List<Record<Event>>> failureHandler = (buffer, resultRecords) -> {};

    List<Record<Event>> result = LambdaCommonHandler.sendRecords(records, config, lambdaAsyncClient, outputCodecContext, successHandler, failureHandler);

    assertNotNull(result);
    verify(lambdaAsyncClient, atLeastOnce()).invoke(any(InvokeRequest.class));
  }
}