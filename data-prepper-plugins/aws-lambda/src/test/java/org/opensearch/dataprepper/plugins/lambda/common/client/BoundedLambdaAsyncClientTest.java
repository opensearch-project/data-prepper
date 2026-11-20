/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.lambda.common.client;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.services.lambda.model.InvokeWithResponseStreamRequest;
import software.amazon.awssdk.services.lambda.model.InvokeWithResponseStreamResponseHandler;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BoundedLambdaAsyncClientTest {

    @Mock
    private LambdaAsyncClient delegateClient;

    private int maxConcurrency;
    private InvokeRequest invokeRequest;
    private ExecutorService executorService;

    @BeforeEach
    void setUp() {
        maxConcurrency = ThreadLocalRandom.current().nextInt(2, 10);
        invokeRequest = InvokeRequest.builder()
                .functionName(UUID.randomUUID().toString())
                .build();
        executorService = Executors.newCachedThreadPool();
    }

    @AfterEach
    void tearDown() {
        executorService.shutdownNow();
    }

    private BoundedLambdaAsyncClient createObjectUnderTest() {
        return new BoundedLambdaAsyncClient(delegateClient, maxConcurrency);
    }

    @Test
    void invoke_returns_future_completing_with_delegate_result() {
        final InvokeResponse expectedResponse = InvokeResponse.builder().statusCode(200).build();
        when(delegateClient.invoke(any(InvokeRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(expectedResponse));

        final InvokeResponse actualResponse = createObjectUnderTest().invoke(invokeRequest).join();

        assertThat(actualResponse, sameInstance(expectedResponse));
        verify(delegateClient).invoke(invokeRequest);
    }

    @Test
    void invoke_releases_permit_after_success() {
        when(delegateClient.invoke(any(InvokeRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(InvokeResponse.builder().statusCode(200).build()));
        final BoundedLambdaAsyncClient objectUnderTest = createObjectUnderTest();

        for (int call = 0; call < maxConcurrency * 2; call++) {
            objectUnderTest.invoke(invokeRequest).join();
        }

        assertThat(objectUnderTest.getAvailableConcurrency(), equalTo(maxConcurrency));
    }

    @Test
    void invoke_releases_permit_after_failure() {
        when(delegateClient.invoke(any(InvokeRequest.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException(UUID.randomUUID().toString())));
        final BoundedLambdaAsyncClient objectUnderTest = createObjectUnderTest();

        for (int call = 0; call < maxConcurrency * 2; call++) {
            assertThrows(CompletionException.class, () -> objectUnderTest.invoke(invokeRequest).join());
        }

        assertThat(objectUnderTest.getAvailableConcurrency(), equalTo(maxConcurrency));
    }

    @Test
    void invoke_releases_permit_when_delegate_throws_synchronously() {
        final RuntimeException delegateException = new RuntimeException(UUID.randomUUID().toString());
        when(delegateClient.invoke(any(InvokeRequest.class))).thenThrow(delegateException);
        final BoundedLambdaAsyncClient objectUnderTest = createObjectUnderTest();

        final RuntimeException actualException = assertThrows(RuntimeException.class,
                () -> objectUnderTest.invoke(invokeRequest));

        assertThat(actualException, sameInstance(delegateException));
        assertThat(objectUnderTest.getAvailableConcurrency(), equalTo(maxConcurrency));
    }

    @Test
    void invoke_blocks_when_all_permits_are_held_and_proceeds_after_a_release() throws Exception {
        final CompletableFuture<InvokeResponse> inFlightFuture = new CompletableFuture<>();
        when(delegateClient.invoke(any(InvokeRequest.class))).thenReturn(inFlightFuture);
        final BoundedLambdaAsyncClient objectUnderTest = createObjectUnderTest();

        for (int call = 0; call < maxConcurrency; call++) {
            objectUnderTest.invoke(invokeRequest);
        }
        assertThat(objectUnderTest.getAvailableConcurrency(), equalTo(0));

        final Future<CompletableFuture<InvokeResponse>> blockedInvoke =
                executorService.submit(() -> objectUnderTest.invoke(invokeRequest));

        await().during(Duration.ofMillis(200))
                .atMost(Duration.ofSeconds(1))
                .until(() -> !blockedInvoke.isDone());
        verify(delegateClient, times(maxConcurrency)).invoke(any(InvokeRequest.class));

        inFlightFuture.complete(InvokeResponse.builder().statusCode(200).build());

        await().atMost(Duration.ofSeconds(5))
                .until(blockedInvoke::isDone);
        assertThat(blockedInvoke.get().join().statusCode(), equalTo(200));
    }

    @Test
    void invoke_never_exceeds_max_concurrency_under_contention() throws Exception {
        final CompletableFuture<InvokeResponse> inFlightFuture = new CompletableFuture<>();
        when(delegateClient.invoke(any(InvokeRequest.class))).thenReturn(inFlightFuture);
        final BoundedLambdaAsyncClient objectUnderTest = createObjectUnderTest();

        final int totalCalls = maxConcurrency * 3;
        for (int call = 0; call < totalCalls; call++) {
            executorService.submit(() -> objectUnderTest.invoke(invokeRequest));
        }

        await().atMost(Duration.ofSeconds(5))
                .until(() -> objectUnderTest.getAvailableConcurrency() == 0);
        verify(delegateClient, atMost(maxConcurrency)).invoke(any(InvokeRequest.class));

        inFlightFuture.complete(InvokeResponse.builder().statusCode(200).build());

        await().atMost(Duration.ofSeconds(5))
                .until(() -> objectUnderTest.getAvailableConcurrency() == maxConcurrency);
        verify(delegateClient, times(totalCalls)).invoke(any(InvokeRequest.class));
    }

    @Test
    void invokeWithResponseStream_releases_permit_after_completion() {
        when(delegateClient.invokeWithResponseStream(any(InvokeWithResponseStreamRequest.class),
                any(InvokeWithResponseStreamResponseHandler.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        final BoundedLambdaAsyncClient objectUnderTest = createObjectUnderTest();
        final InvokeWithResponseStreamRequest streamRequest = InvokeWithResponseStreamRequest.builder()
                .functionName(UUID.randomUUID().toString())
                .build();

        objectUnderTest.invokeWithResponseStream(streamRequest,
                mock(InvokeWithResponseStreamResponseHandler.class)).join();

        assertThat(objectUnderTest.getAvailableConcurrency(), equalTo(maxConcurrency));
    }

    @Test
    void getMaxConcurrency_returns_configured_value() {
        assertThat(createObjectUnderTest().getMaxConcurrency(), equalTo(maxConcurrency));
    }

    @Test
    void getDelegate_returns_wrapped_client() {
        assertThat(createObjectUnderTest().getDelegate(), sameInstance(delegateClient));
    }
}
