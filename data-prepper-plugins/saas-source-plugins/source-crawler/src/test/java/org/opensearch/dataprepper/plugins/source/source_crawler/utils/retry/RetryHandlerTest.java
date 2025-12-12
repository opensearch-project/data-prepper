/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.source_crawler.utils.retry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.source.source_crawler.exception.SaaSCrawlerException;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RetryHandlerTest {

    private static final int MAX_RETRIES = 3;
    private static final long SLEEP_TIME_MS = 100L;
    @Mock
    private RetryStrategy retryStrategy;
    @Mock
    private StatusCodeHandler statusCodeHandler;
    @Mock
    private Runnable credentialRenewal;
    private RetryHandler retryHandler;

    @BeforeEach
    void setUp() {
        lenient().when(retryStrategy.getMaxRetries()).thenReturn(MAX_RETRIES);
        lenient().when(retryStrategy.calculateSleepTime(any(Exception.class), anyInt()))
                .thenReturn(SLEEP_TIME_MS);
        retryHandler = new RetryHandler(retryStrategy, statusCodeHandler);
    }

    @Test
    void constructor_WithValidParams_InitializesSuccessfully() {
        final RetryHandler handler = new RetryHandler(retryStrategy, statusCodeHandler);
        assertThat(handler, notNullValue());
    }

    @Test
    void executeWithRetry_WithNullOperation_ThrowsIllegalArgumentException() {
        final SaaSCrawlerException exception = assertThrows(SaaSCrawlerException.class,
                () -> retryHandler.executeWithRetry(null, credentialRenewal));

        assertThat(exception.getMessage(), equalTo("Operation cannot be null"));
    }

    @Test
    void executeWithRetry_WithNullCredentialRenewal_ThrowsIllegalArgumentException() {
        final Supplier<String> operation = () -> "success";

        final SaaSCrawlerException exception = assertThrows(SaaSCrawlerException.class,
                () -> retryHandler.executeWithRetry(operation, null));

        assertThat(exception.getMessage(), equalTo("Credential renewal cannot be null"));
    }

    @Test
    void executeWithRetry_WithSuccessfulOperation_ReturnsResultImmediately() {
        final String expectedResult = "success";
        final Supplier<String> operation = () -> expectedResult;

        final String result = retryHandler.executeWithRetry(operation, credentialRenewal);

        assertThat(result, equalTo(expectedResult));
        verify(statusCodeHandler, never()).handleStatusCode(any(), anyInt(), any());
    }

    @Test
    void executeWithRetry_WithRetryableError_RetriesAndSucceeds() {
        final AtomicInteger attemptCount = new AtomicInteger(0);
        final Supplier<String> operation = () -> {
            if (attemptCount.getAndIncrement() < 2) {
                throw new HttpServerErrorException(HttpStatus.SERVICE_UNAVAILABLE);
            }
            return "success";
        };

        when(statusCodeHandler.handleStatusCode(any(HttpServerErrorException.class), anyInt(),
                eq(credentialRenewal))).thenReturn(RetryDecision.retry());

        final String result = retryHandler.executeWithRetry(operation, credentialRenewal);

        assertThat(result, equalTo("success"));
        assertThat(attemptCount.get(), equalTo(3));
        verify(statusCodeHandler, times(2)).handleStatusCode(any(HttpServerErrorException.class),
                anyInt(), eq(credentialRenewal));
    }

    @Test
    void executeWithRetry_WithNonRetryableError_ThrowsExceptionImmediately() {
        final HttpClientErrorException clientException = new HttpClientErrorException(
                HttpStatus.BAD_REQUEST, "Bad Request");
        final Supplier<String> operation = () -> {
            throw clientException;
        };

        when(statusCodeHandler.handleStatusCode(eq(clientException), eq(0),
                eq(credentialRenewal))).thenReturn(RetryDecision.stop());

        final HttpClientErrorException exception = assertThrows(HttpClientErrorException.class,
                () -> retryHandler.executeWithRetry(operation, credentialRenewal));

        assertThat(exception, equalTo(clientException));
        verify(statusCodeHandler, times(1)).handleStatusCode(eq(clientException), eq(0),
                eq(credentialRenewal));
    }

    @Test
    void executeWithRetry_WithStopDecisionAndException_ThrowsCustomException() {
        final HttpClientErrorException clientException = new HttpClientErrorException(
                HttpStatus.FORBIDDEN, "Forbidden");
        final SecurityException customException = new SecurityException("Access denied");
        final Supplier<String> operation = () -> {
            throw clientException;
        };

        when(statusCodeHandler.handleStatusCode(eq(clientException), eq(0),
                eq(credentialRenewal)))
                .thenReturn(RetryDecision.stopWithException(customException));

        final SecurityException exception = assertThrows(SecurityException.class,
                () -> retryHandler.executeWithRetry(operation, credentialRenewal));

        assertThat(exception.getMessage(), equalTo("Access forbidden: Access denied"));
    }

    @Test
    void executeWithRetry_ExceedingMaxRetries_ThrowsHttpServerErrorException() {
        final Supplier<String> operation = () -> {
            throw new HttpServerErrorException(HttpStatus.SERVICE_UNAVAILABLE);
        };

        when(statusCodeHandler.handleStatusCode(any(HttpServerErrorException.class), anyInt(),
                eq(credentialRenewal))).thenReturn(RetryDecision.retry());

        final HttpServerErrorException exception = assertThrows(HttpServerErrorException.class,
                () -> retryHandler.executeWithRetry(operation, credentialRenewal));

        assertThat(exception.getStatusCode(), equalTo(HttpStatus.SERVICE_UNAVAILABLE));
        verify(statusCodeHandler, times(MAX_RETRIES)).handleStatusCode(
                any(HttpServerErrorException.class), anyInt(), eq(credentialRenewal));
    }

    @Test
    void executeWithRetry_WithHttpClientErrorException_HandlesCorrectly() {
        final AtomicInteger attemptCount = new AtomicInteger(0);
        final HttpClientErrorException clientException = new HttpClientErrorException(
                HttpStatus.UNAUTHORIZED);
        final Supplier<String> operation = () -> {
            if (attemptCount.getAndIncrement() < 1) {
                throw clientException;
            }
            return "success";
        };

        when(statusCodeHandler.handleStatusCode(eq(clientException), eq(0),
                eq(credentialRenewal))).thenReturn(RetryDecision.retry());

        final String result = retryHandler.executeWithRetry(operation, credentialRenewal);

        assertThat(result, equalTo("success"));
        verify(statusCodeHandler, times(1)).handleStatusCode(eq(clientException), eq(0),
                eq(credentialRenewal));
        verify(credentialRenewal, never()).run();
    }

    @Test
    void executeWithRetry_WithMultipleRetries_UsesCorrectRetryCount() {
        final AtomicInteger attemptCount = new AtomicInteger(0);
        final Supplier<String> operation = () -> {
            if (attemptCount.getAndIncrement() < 2) {
                throw new HttpServerErrorException(HttpStatus.INTERNAL_SERVER_ERROR);
            }
            return "success";
        };

        when(statusCodeHandler.handleStatusCode(any(HttpServerErrorException.class), eq(0),
                eq(credentialRenewal))).thenReturn(RetryDecision.retry());
        when(statusCodeHandler.handleStatusCode(any(HttpServerErrorException.class), eq(1),
                eq(credentialRenewal))).thenReturn(RetryDecision.retry());

        final String result = retryHandler.executeWithRetry(operation, credentialRenewal);

        assertThat(result, equalTo("success"));
        verify(statusCodeHandler, times(1)).handleStatusCode(any(HttpServerErrorException.class),
                eq(0), eq(credentialRenewal));
        verify(statusCodeHandler, times(1)).handleStatusCode(any(HttpServerErrorException.class),
                eq(1), eq(credentialRenewal));
    }

    @Test
    void executeWithRetry_WithRateLimitError_RetriesWithBackoff() {
        final AtomicInteger attemptCount = new AtomicInteger(0);
        final HttpClientErrorException rateLimitException = new HttpClientErrorException(
                HttpStatus.TOO_MANY_REQUESTS);
        final Supplier<String> operation = () -> {
            if (attemptCount.getAndIncrement() < 1) {
                throw rateLimitException;
            }
            return "success";
        };

        when(retryStrategy.calculateSleepTime(eq(rateLimitException), eq(0)))
                .thenReturn(5000L);
        when(statusCodeHandler.handleStatusCode(eq(rateLimitException), eq(0),
                eq(credentialRenewal))).thenReturn(RetryDecision.retry());

        final String result = retryHandler.executeWithRetry(operation, credentialRenewal);

        assertThat(result, equalTo("success"));
        verify(retryStrategy, times(1)).calculateSleepTime(eq(rateLimitException), eq(0));
    }

    @Test
    void executeWithRetry_WithInterruptedException_ThrowsRuntimeException() {
        final Supplier<String> operation = () -> {
            throw new HttpServerErrorException(HttpStatus.SERVICE_UNAVAILABLE);
        };

        when(statusCodeHandler.handleStatusCode(any(HttpServerErrorException.class), anyInt(),
                eq(credentialRenewal))).thenReturn(RetryDecision.retry());
        when(retryStrategy.calculateSleepTime(any(Exception.class), anyInt()))
                .thenAnswer(invocation -> {
                    Thread.currentThread().interrupt();
                    return SLEEP_TIME_MS;
                });

        final RuntimeException exception = assertThrows(RuntimeException.class,
                () -> retryHandler.executeWithRetry(operation, credentialRenewal));

        assertThat(exception.getMessage(), equalTo("Retry interrupted"));
        assertThat(exception.getCause(), instanceOf(InterruptedException.class));
    }

    @Test
    void executeWithRetry_WithNotFoundError_StopsRetrying() {
        final HttpClientErrorException notFoundException = new HttpClientErrorException(
                HttpStatus.NOT_FOUND);
        final Supplier<String> operation = () -> {
            throw notFoundException;
        };

        when(statusCodeHandler.handleStatusCode(eq(notFoundException), eq(0),
                eq(credentialRenewal))).thenReturn(RetryDecision.stop());

        final HttpClientErrorException exception = assertThrows(HttpClientErrorException.class,
                () -> retryHandler.executeWithRetry(operation, credentialRenewal));

        assertThat(exception, equalTo(notFoundException));
        verify(statusCodeHandler, times(1)).handleStatusCode(eq(notFoundException), eq(0),
                eq(credentialRenewal));
    }

    @Test
    void executeWithRetry_WithSuccessAfterOneRetry_CallsOperationTwice() {
        final AtomicInteger attemptCount = new AtomicInteger(0);
        final Supplier<String> operation = () -> {
            if (attemptCount.getAndIncrement() == 0) {
                throw new HttpServerErrorException(HttpStatus.BAD_GATEWAY);
            }
            return "success";
        };

        when(statusCodeHandler.handleStatusCode(any(HttpServerErrorException.class), eq(0),
                eq(credentialRenewal))).thenReturn(RetryDecision.retry());

        final String result = retryHandler.executeWithRetry(operation, credentialRenewal);

        assertThat(result, equalTo("success"));
        assertThat(attemptCount.get(), equalTo(2));
    }

    @Test
    void executeWithRetry_WithDifferentExceptionTypes_HandlesCorrectly() {
        final AtomicInteger attemptCount = new AtomicInteger(0);
        final Supplier<String> operation = () -> {
            final int attempt = attemptCount.getAndIncrement();
            if (attempt == 0) {
                throw new HttpClientErrorException(HttpStatus.UNAUTHORIZED);
            } else if (attempt == 1) {
                throw new HttpServerErrorException(HttpStatus.SERVICE_UNAVAILABLE);
            }
            return "success";
        };

        when(statusCodeHandler.handleStatusCode(any(HttpClientErrorException.class), eq(0),
                eq(credentialRenewal))).thenReturn(RetryDecision.retry());
        when(statusCodeHandler.handleStatusCode(any(HttpServerErrorException.class), eq(1),
                eq(credentialRenewal))).thenReturn(RetryDecision.retry());

        final String result = retryHandler.executeWithRetry(operation, credentialRenewal);

        assertThat(result, equalTo("success"));
        assertThat(attemptCount.get(), equalTo(3));
    }

    @Test
    void executeWithRetry_WithMaxRetriesReached_VerifiesRetryCount() {
        when(retryStrategy.getMaxRetries()).thenReturn(2);
        final RetryHandler handlerWithLimitedRetries = new RetryHandler(retryStrategy,
                statusCodeHandler);
        final Supplier<String> operation = () -> {
            throw new HttpServerErrorException(HttpStatus.INTERNAL_SERVER_ERROR);
        };

        when(statusCodeHandler.handleStatusCode(any(HttpServerErrorException.class), anyInt(),
                eq(credentialRenewal))).thenReturn(RetryDecision.retry());

        final HttpServerErrorException exception = assertThrows(HttpServerErrorException.class,
                () -> handlerWithLimitedRetries.executeWithRetry(operation, credentialRenewal));

        assertThat(exception.getStatusCode(), equalTo(HttpStatus.INTERNAL_SERVER_ERROR));
        verify(statusCodeHandler, times(2)).handleStatusCode(any(HttpServerErrorException.class),
                anyInt(), eq(credentialRenewal));
    }
}
