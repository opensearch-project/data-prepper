/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kinesis.source.apihandler;

import com.linecorp.armeria.client.retry.Backoff;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.kinesis.source.exceptions.KinesisRetriesExhaustedException;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KinesisClientApiRetryHandlerTest {
    private static final String TEST_OPERATION = "testOperation";
    private static final int MAX_RETRY_COUNT = 3;
    private static final long DELAY_MILLIS = 100L;

    @Mock
    private Backoff backoff;

    @Mock
    private KinesisClientApiRetryHandler.ExceptionHandler exceptionHandler;

    private KinesisClientApiRetryHandler kinesisClientApiRetryHandler;

    @BeforeEach
    void setUp() {
        kinesisClientApiRetryHandler = new KinesisClientApiRetryHandler(backoff, MAX_RETRY_COUNT);
    }

    @Test
    void constructor_withZeroMaxRetryCount_throwsException() {
        assertThrows(IllegalArgumentException.class,
                () -> new KinesisClientApiRetryHandler(backoff, 0),
                "Maximum Retry count should be strictly greater than zero.");
    }

    @Test
    void constructor_withNegativeMaxRetryCount_throwsException() {
        assertThrows(IllegalArgumentException.class,
                () -> new KinesisClientApiRetryHandler(backoff, -1),
                "Maximum Retry count should be strictly greater than zero.");
    }

    @Test
    void executeWithRetry_successOnFirstAttempt_returnsResult() {
        String expectedResult = "success";
        String result = kinesisClientApiRetryHandler.executeWithRetry(
                TEST_OPERATION,
                () -> expectedResult,
                exceptionHandler
        );

        assertEquals(expectedResult, result);
        verify(backoff, never()).nextDelayMillis(anyInt());
        verify(exceptionHandler, never()).handle(any(), anyInt());
    }

    @Test
    void executeWithRetry_successOnSecondAttempt_returnsResult() {
        when(backoff.nextDelayMillis(0)).thenReturn(DELAY_MILLIS);
        AtomicInteger attempts = new AtomicInteger(0);
        String expectedResult = "success";

        String result = kinesisClientApiRetryHandler.executeWithRetry(
                TEST_OPERATION,
                () -> {
                    if (attempts.getAndIncrement() == 0) {
                        throw new RuntimeException("First attempt failed");
                    }
                    return expectedResult;
                },
                exceptionHandler
        );

        assertEquals(expectedResult, result);
        verify(backoff, times(1)).nextDelayMillis(0);
        verify(exceptionHandler, times(1)).handle(any(), eq(0));
    }

    @Test
    void executeWithRetry_allAttemptsFail_throwsException() {
        when(backoff.nextDelayMillis(anyInt())).thenReturn(DELAY_MILLIS);
        RuntimeException testException = new RuntimeException("Test failure");

        KinesisRetriesExhaustedException exception = assertThrows(
                KinesisRetriesExhaustedException.class,
                () -> kinesisClientApiRetryHandler.executeWithRetry(
                        TEST_OPERATION,
                        () -> { throw testException; },
                        exceptionHandler
                )
        );

        assertEquals(
                String.format("Failed to execute %s after %d retries", TEST_OPERATION, MAX_RETRY_COUNT),
                exception.getMessage()
        );
        verify(backoff, times(MAX_RETRY_COUNT)).nextDelayMillis(anyInt());
        verify(exceptionHandler, times(MAX_RETRY_COUNT)).handle(any(), anyInt());
    }

    @Test
    void executeWithRetry_negativeBackoffDelay_throwsException() {
        when(backoff.nextDelayMillis(0)).thenReturn(-1L);
        RuntimeException testException = new RuntimeException("Test failure");

        KinesisRetriesExhaustedException exception = assertThrows(
                KinesisRetriesExhaustedException.class,
                () -> kinesisClientApiRetryHandler.executeWithRetry(
                        TEST_OPERATION,
                        () -> { throw testException; },
                        exceptionHandler
                )
        );

        assertEquals(
                "Retries exhausted. Make sure that configuration is valid and required permissions are present.",
                exception.getMessage()
        );
        verify(backoff, times(1)).nextDelayMillis(0);
        verify(exceptionHandler, times(1)).handle(any(), eq(0));
    }

    @Test
    void executeWithRetry_interruptedDuringSleep_throwsException() {
        when(backoff.nextDelayMillis(0)).thenReturn(DELAY_MILLIS);
        RuntimeException testException = new RuntimeException("Test failure");
        Thread.currentThread().interrupt();

        KinesisRetriesExhaustedException exception = assertThrows(
                KinesisRetriesExhaustedException.class,
                () -> kinesisClientApiRetryHandler.executeWithRetry(
                        TEST_OPERATION,
                        () -> { throw testException; },
                        exceptionHandler
                )
        );


        verify(backoff, times(1)).nextDelayMillis(0);
        verify(exceptionHandler, times(1)).handle(any(), eq(0));
        // Clear interrupted state
        assertTrue(Thread.interrupted());
    }

    @Test
    void executeWithRetry_nullOperation_throwsException() {
        assertThrows(NullPointerException.class,
                () -> kinesisClientApiRetryHandler.executeWithRetry(TEST_OPERATION, null, exceptionHandler),
                "Operation cannot be null");
    }

    @Test
    void executeWithRetry_nullExceptionHandler_throwsException() {
        assertThrows(NullPointerException.class,
                () -> kinesisClientApiRetryHandler.executeWithRetry(TEST_OPERATION, () -> "result", null),
                "Exception handler cannot be null");
    }

    @Test
    void executeWithRetry_nullOperationName_throwsException() {
        assertThrows(NullPointerException.class,
                () -> kinesisClientApiRetryHandler.executeWithRetry(null, () -> "result", exceptionHandler),
                "Operation name cannot be null");
    }

    @Test
    void constructor_nullBackoff_throwsException() {
        assertThrows(NullPointerException.class,
                () -> new KinesisClientApiRetryHandler(null, MAX_RETRY_COUNT),
                "Backoff cannot be null");
    }
    @Test
    void executeWithRetry_operationReturnsNull_returnsNull() {
        Supplier<String> nullSupplier = () -> null;
        String result = kinesisClientApiRetryHandler.executeWithRetry(TEST_OPERATION, nullSupplier, exceptionHandler);
        assertNull(result);
        verify(backoff, never()).nextDelayMillis(anyInt());
        verify(exceptionHandler, never()).handle(any(), anyInt());
    }

    @Test
    void executeWithRetry_successAfterMultipleRetries_returnsResult() {
        when(backoff.nextDelayMillis(anyInt())).thenReturn(DELAY_MILLIS);
        AtomicInteger attempts = new AtomicInteger(0);
        String expectedResult = "success";

        String result = kinesisClientApiRetryHandler.executeWithRetry(
                TEST_OPERATION,
                () -> {
                    int attempt = attempts.getAndIncrement();
                    if (attempt < 2) {
                        throw new RuntimeException("Attempt " + attempt + " failed");
                    }
                    return expectedResult;
                },
                exceptionHandler
        );

        assertEquals(expectedResult, result);
        verify(backoff, times(2)).nextDelayMillis(anyInt());
        verify(exceptionHandler, times(2)).handle(any(), anyInt());
    }

    @Test
    void executeWithRetry_differentExceptionTypes_handlesAllTypes() {
        when(backoff.nextDelayMillis(anyInt())).thenReturn(DELAY_MILLIS);
        AtomicInteger attempts = new AtomicInteger(0);

        assertThrows(KinesisRetriesExhaustedException.class, () ->
                kinesisClientApiRetryHandler.executeWithRetry(
                        TEST_OPERATION,
                        () -> {
                            switch (attempts.getAndIncrement()) {
                                case 0:
                                    throw new IllegalArgumentException("Invalid argument");
                                case 1:
                                    throw new RuntimeException("Runtime error");
                                default:
                                    throw new IllegalStateException("Invalid state");
                            }
                        },
                        exceptionHandler
                )
        );

        verify(backoff, times(MAX_RETRY_COUNT)).nextDelayMillis(anyInt());
        verify(exceptionHandler, times(MAX_RETRY_COUNT)).handle(any(), anyInt());
    }
}
