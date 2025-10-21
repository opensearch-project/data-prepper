/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.common.utils;

import com.linecorp.armeria.client.retry.Backoff;
import org.slf4j.Logger;

import java.util.function.Predicate;

public class RetryUtil {
    // Default values
    private static final long DEFAULT_BASE_DELAY_MS = 100;   // 100ms base delay
    private static final long DEFAULT_MAX_DELAY_MS = 1000;    // 1 second max delay
    private static final int DEFAULT_MAX_RETRIES = 3;         // Default max retries

    /**
     * Represents the result of a retryable operation
     */
    public static class RetryResult {
        private final boolean success;
        private final Exception lastException;
        private final int attemptsMade;

        public RetryResult(boolean success, Exception lastException, int attemptsMade) {
            this.success = success;
            this.lastException = lastException;
            this.attemptsMade = attemptsMade;
        }

        public boolean isSuccess() { return success; }
        public Exception getLastException() { return lastException; }
        public int getAttemptsMade() { return attemptsMade; }
    }

    /**
     * Retry with exponential backoff, using default values.
     */
    public static boolean retryWithBackoff(Runnable task, Logger log) {
        return retryWithBackoff(task, DEFAULT_BASE_DELAY_MS, DEFAULT_MAX_DELAY_MS, DEFAULT_MAX_RETRIES, log);
    }

    /**
     * Retry with exponential backoff, allowing custom delay and retry values.
     */
    public static boolean retryWithBackoff(Runnable task, long baseDelayMs, long maxDelayMs, int maxRetries, Logger log) {
        RetryResult result = retryWithBackoffInternal(task, baseDelayMs, maxDelayMs, maxRetries, e -> true, log);
        return result.isSuccess();
    }

    /**
     * Retry with exponential backoff, returning detailed result.
     */
    public static RetryResult retryWithBackoffWithResult(Runnable task, Logger log) {
        return retryWithBackoffInternal(task, DEFAULT_BASE_DELAY_MS, DEFAULT_MAX_DELAY_MS, DEFAULT_MAX_RETRIES, e -> true, log);
    }

    private static RetryResult retryWithBackoffInternal(
            Runnable task,
            long baseDelayMs,
            long maxDelayMs,
            int maxRetries,
            Predicate<Exception> retryPredicate,
            Logger log) {
        final Backoff backoff = Backoff.exponential(baseDelayMs, maxDelayMs)
                .withMaxAttempts(maxRetries);
        int attempt = 1;
        Exception lastException = null;

        while (true) {
            try {
                task.run();
                return new RetryResult(true, null, attempt);
            } catch (Exception e) {
                lastException = e;

                if (!retryPredicate.test(e)) {
                    return new RetryResult(false, e, attempt);
                }

                long delayMillis = backoff.nextDelayMillis(attempt++);
                if (delayMillis < 0) {
                    log.error("Max retries ({}) reached. Last exception: {}", maxRetries, e.getMessage(), e);
                    return new RetryResult(false, e, attempt - 1);
                }

                log.warn("Retry attempt {} failed. Retrying in {} ms... Error: {}",
                        attempt - 1, delayMillis, e.getMessage());

                try {
                    Thread.sleep(delayMillis);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    log.error("Retry thread interrupted", ie);
                    return new RetryResult(false, ie, attempt - 1);
                }
            }
        }
    }
}
