package org.opensearch.dataprepper.plugins.lambda.utils;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.dataprepper.plugins.lambda.common.accumlator.Buffer;
import org.opensearch.dataprepper.plugins.lambda.common.config.ClientOptions;
import org.opensearch.dataprepper.plugins.lambda.common.config.InvocationType;
import org.opensearch.dataprepper.plugins.lambda.common.config.LambdaCommonConfig;
import org.opensearch.dataprepper.plugins.lambda.common.util.LambdaRetryStrategy;
import org.slf4j.Logger;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.services.lambda.model.LambdaException;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class LambdaRetryStrategyTest {

    @Mock
    private LambdaAsyncClient lambdaAsyncClient;

    @Mock
    private Buffer buffer;

    @Mock
    private LambdaCommonConfig config;

    @Mock
    private Logger logger;

    @BeforeEach
    void setUp() {
//        when(lambdaAsyncClient.invoke(any(InvokeRequest.class))).thenReturn(CompletableFuture.completedFuture(InvokeResponse.builder().statusCode(200).build()));
        when(config.getClientOptions()).thenReturn(mock(ClientOptions.class));
        when(config.getClientOptions().getMaxConnectionRetries()).thenReturn(3);
        when(config.getClientOptions().getBaseDelay()).thenReturn(Duration.ofMillis(100));
        when(config.getFunctionName()).thenReturn("testFunction");
        when(config.getInvocationType()).thenReturn(InvocationType.REQUEST_RESPONSE);
    }

    @Test
    void testIsRetryable() {
        assertTrue(LambdaRetryStrategy.isRetryableStatusCode(429));
        assertTrue(LambdaRetryStrategy.isRetryableStatusCode(500));
        assertFalse(LambdaRetryStrategy.isRetryableStatusCode(200));
    }

    @Test
    void testIsNonRetryable() {
        assertTrue(LambdaRetryStrategy.isNonRetryable(InvokeResponse.builder().statusCode(400).build()));
        assertTrue(LambdaRetryStrategy.isNonRetryable(InvokeResponse.builder().statusCode(403).build()));
        assertFalse(LambdaRetryStrategy.isNonRetryable(InvokeResponse.builder().statusCode(500).build()));
        assertFalse(LambdaRetryStrategy.isNonRetryable(null));
    }

    @Test
    void testIsTimeoutError() {
        assertTrue(LambdaRetryStrategy.isTimeoutError(InvokeResponse.builder().statusCode(408).build()));
        assertTrue(LambdaRetryStrategy.isTimeoutError(InvokeResponse.builder().statusCode(429).build()));
        assertFalse(LambdaRetryStrategy.isTimeoutError(InvokeResponse.builder().statusCode(200).build()));
    }

    private Throwable createAcquireTimeoutFailure() {
        // Reproduces the exact shape the AWS SDK Netty client produces: the connection pool's
        // JDK TimeoutException, wrapped in an untyped Throwable, wrapped in SdkClientException.
        final TimeoutException poolTimeout =
                new TimeoutException("Acquire operation took longer than configured maximum time");
        final Throwable decorated = new Throwable(
                "Acquire operation took longer than the configured maximum time. This indicates that a " +
                        "request cannot get a connection from the pool within the specified maximum time.",
                poolTimeout);
        return SdkClientException.create("Unable to execute HTTP request: " + decorated.getMessage(), decorated);
    }

    @Test
    void isConnectionPoolAcquireFailure_returns_true_for_direct_sdk_client_exception() {
        final Throwable failure = new CompletionException(createAcquireTimeoutFailure());

        assertTrue(LambdaRetryStrategy.isConnectionPoolAcquireFailure(failure));
    }

    @Test
    void isConnectionPoolAcquireFailure_returns_true_when_suppressed_in_api_call_timeout() {
        final ApiCallTimeoutException apiCallTimeout = ApiCallTimeoutException.create(60000L);
        apiCallTimeout.addSuppressed(SdkClientException.create(
                "Request attempt 1 failure", createAcquireTimeoutFailure()));

        assertFalse(LambdaRetryStrategy.isConnectionPoolAcquireFailure(ApiCallTimeoutException.create(60000L)));
        assertTrue(LambdaRetryStrategy.isConnectionPoolAcquireFailure(new CompletionException(apiCallTimeout)));
    }

    @Test
    void isConnectionPoolAcquireFailure_returns_false_for_service_exceptions() {
        final AwsServiceException serviceException = LambdaException.builder()
                .message("Acquire operation took longer - a misleading service message")
                .statusCode(400)
                .build();

        assertFalse(LambdaRetryStrategy.isConnectionPoolAcquireFailure(new CompletionException(serviceException)));
    }

    @Test
    void isConnectionPoolAcquireFailure_returns_false_for_other_timeouts_and_null() {
        assertFalse(LambdaRetryStrategy.isConnectionPoolAcquireFailure(
                new CompletionException(SdkClientException.create("Unable to execute HTTP request",
                        new TimeoutException("some other timeout")))));
        assertFalse(LambdaRetryStrategy.isConnectionPoolAcquireFailure(null));
    }

    private ClientOptions fastBackoffClientOptions() {
        final ClientOptions clientOptions = mock(ClientOptions.class);
        when(clientOptions.getBaseDelay()).thenReturn(Duration.ofMillis(1));
        when(clientOptions.getMaxBackoff()).thenReturn(Duration.ofMillis(5));
        return clientOptions;
    }

    @Test
    void joinWithConnectionPoolBackoff_returns_result_without_retrying_on_success() {
        final InvokeResponse expectedResponse = InvokeResponse.builder().statusCode(200).build();
        final Counter poolExhaustionCounter = mock(Counter.class);

        final InvokeResponse actualResponse = LambdaRetryStrategy.joinWithConnectionPoolBackoff(
                CompletableFuture.completedFuture(expectedResponse),
                () -> {
                    throw new IllegalStateException("retry should not run on success");
                },
                fastBackoffClientOptions(),
                poolExhaustionCounter);

        assertThat(actualResponse, sameInstance(expectedResponse));
        verifyNoInteractions(poolExhaustionCounter);
    }

    @Test
    void joinWithConnectionPoolBackoff_rethrows_when_failure_is_not_pool_exhaustion() {
        final RuntimeException otherFailure = new RuntimeException("not pool exhaustion");
        final Counter poolExhaustionCounter = mock(Counter.class);

        final CompletionException actualException = assertThrows(CompletionException.class,
                () -> LambdaRetryStrategy.joinWithConnectionPoolBackoff(
                        CompletableFuture.failedFuture(otherFailure),
                        () -> {
                            throw new IllegalStateException("retry should not run for other failures");
                        },
                        fastBackoffClientOptions(),
                        poolExhaustionCounter));

        assertThat(actualException.getCause(), sameInstance(otherFailure));
        verifyNoInteractions(poolExhaustionCounter);
    }

    @Test
    void joinWithConnectionPoolBackoff_returns_result_of_successful_retry_and_counts_each_failed_attempt() {
        final InvokeResponse expectedResponse = InvokeResponse.builder().statusCode(200).build();
        final Counter poolExhaustionCounter = mock(Counter.class);
        final AtomicInteger retryAttempts = new AtomicInteger();

        final InvokeResponse actualResponse = LambdaRetryStrategy.joinWithConnectionPoolBackoff(
                CompletableFuture.failedFuture(createAcquireTimeoutFailure()),
                () -> retryAttempts.incrementAndGet() < 3
                        ? CompletableFuture.failedFuture(createAcquireTimeoutFailure())
                        : CompletableFuture.completedFuture(expectedResponse),
                fastBackoffClientOptions(),
                poolExhaustionCounter);

        assertThat(actualResponse, sameInstance(expectedResponse));
        assertThat(retryAttempts.get(), equalTo(3));
        // One count for the initial failure and one for each of the two failed retries.
        verify(poolExhaustionCounter, times(3)).increment();
    }

    @Test
    void joinWithConnectionPoolBackoff_stops_retrying_when_failure_changes_type() {
        final RuntimeException otherFailure = new RuntimeException("not pool exhaustion");
        final Counter poolExhaustionCounter = mock(Counter.class);

        final CompletionException actualException = assertThrows(CompletionException.class,
                () -> LambdaRetryStrategy.joinWithConnectionPoolBackoff(
                        CompletableFuture.failedFuture(createAcquireTimeoutFailure()),
                        () -> CompletableFuture.failedFuture(otherFailure),
                        fastBackoffClientOptions(),
                        poolExhaustionCounter));

        assertThat(actualException.getCause(), sameInstance(otherFailure));
        verify(poolExhaustionCounter, times(1)).increment();
    }

    @Test
    void joinWithConnectionPoolBackoff_throws_last_failure_after_exhausting_retries() {
        final Counter poolExhaustionCounter = mock(Counter.class);
        final AtomicInteger retryAttempts = new AtomicInteger();

        final CompletionException actualException = assertThrows(CompletionException.class,
                () -> LambdaRetryStrategy.joinWithConnectionPoolBackoff(
                        CompletableFuture.failedFuture(createAcquireTimeoutFailure()),
                        () -> {
                            retryAttempts.incrementAndGet();
                            return CompletableFuture.failedFuture(createAcquireTimeoutFailure());
                        },
                        fastBackoffClientOptions(),
                        poolExhaustionCounter));

        assertTrue(LambdaRetryStrategy.isConnectionPoolAcquireFailure(actualException));
        assertThat(retryAttempts.get(), equalTo(10));
        verify(poolExhaustionCounter, times(11)).increment();
    }

    @Test
    void joinWithConnectionPoolBackoff_retries_with_a_delay_when_base_delay_is_zero() {
        final ClientOptions clientOptions = mock(ClientOptions.class);
        when(clientOptions.getBaseDelay()).thenReturn(Duration.ZERO);
        when(clientOptions.getMaxBackoff()).thenReturn(Duration.ZERO);
        final Counter poolExhaustionCounter = mock(Counter.class);
        final InvokeResponse expectedResponse = InvokeResponse.builder().statusCode(200).build();

        final InvokeResponse actualResponse = LambdaRetryStrategy.joinWithConnectionPoolBackoff(
                CompletableFuture.failedFuture(createAcquireTimeoutFailure()),
                () -> CompletableFuture.completedFuture(expectedResponse),
                clientOptions,
                poolExhaustionCounter);

        assertThat(actualResponse, sameInstance(expectedResponse));
    }

}
