/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.source_crawler.utils;

import io.micrometer.core.instrument.Counter;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.dataprepper.plugins.source.source_crawler.utils.retry.RetryDecision;
import org.opensearch.dataprepper.plugins.source.source_crawler.utils.retry.RetryStrategy;
import org.opensearch.dataprepper.plugins.source.source_crawler.utils.retry.StatusCodeHandler;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;

import java.util.Optional;
import java.util.function.Supplier;

@Slf4j
public class RetryHandler {
    private final RetryStrategy retryStrategy;
    private final StatusCodeHandler statusCodeHandler;

    /**
     * Constructor
     *
     * @param retryStrategy     Strategy for determining retry behavior
     * @param statusCodeHandler Handler for HTTP status codes
     */
    public RetryHandler(RetryStrategy retryStrategy, StatusCodeHandler statusCodeHandler) {
        this.retryStrategy = retryStrategy;
        this.statusCodeHandler = statusCodeHandler;
    }

    /**
     * Executes the given operation with retry logic, optional credential renewal,
     * and failure counter.
     *
     * @param operation         The operation to execute.
     * @param credentialRenewal The action to renew credentials if needed.
     *
     * @param <T>               The return type of the operation.
     * @return The result of the operation.
     */
    public <T> T executeWithRetry(Supplier<T> operation, Runnable credentialRenewal) {
        return executeWithRetry(operation, credentialRenewal, Optional.empty());
    }

    /**
     * Executes the given operation with retry logic, optional credential renewal,
     * and failure counter.
     *
     * @param operation         The operation to execute.
     * @param credentialRenewal The action to renew credentials if needed.
     * @param failureCounter    The counter to increment on each failed attempt
     *                          (optional).
     * @param <T>               The return type of the operation.
     * @return The result of the operation.
     */
    public <T> T executeWithRetry(Supplier<T> operation, Runnable credentialRenewal, Optional<Counter> failureCounter) {
        if (operation == null) {
            throw new IllegalArgumentException("Operation cannot be null");
        }
        if (credentialRenewal == null) {
            throw new IllegalArgumentException("Credential renewal cannot be null");
        }

        final int maxRetries = retryStrategy.getMaxRetries();
        int retryCount = 0;

        while (retryCount < maxRetries) {
            boolean operationSucceeded = false;
            try {
                T result = operation.get();
                operationSucceeded = true;
                return result;
            } catch (HttpClientErrorException | HttpServerErrorException ex) {
                RetryDecision decision = statusCodeHandler.handleStatusCode(
                        ex, retryCount, credentialRenewal);

                if (decision.isShouldStop()) {
                    if (decision.getException() != null) {
                        throw decision.getException();
                    }
                    throw ex;
                }

                if (retryCount == maxRetries - 1) {
                    log.error("Exceeded maximum retry attempts ({})", maxRetries, ex);
                    throw ex;
                }

                // Calculate sleep time and wait
                long sleepTimeMs = retryStrategy.calculateSleepTime(ex, retryCount);
                sleep(sleepTimeMs);
            } finally {
                if (!operationSucceeded) {
                    failureCounter.ifPresent(Counter::increment);
                }
            }
            retryCount++;
        }
        throw new RuntimeException("Exceeded maximum retry attempts (" + maxRetries + ")");
    }

    private void sleep(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Retry interrupted", ie);
        }
    }
}
