/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.common.utils;

import com.linecorp.armeria.client.retry.Backoff;
import org.slf4j.Logger;

public class RetryUtil {
    // Default values
    private static final long DEFAULT_BASE_DELAY_MS = 100;   // 100ms base delay
    private static final long DEFAULT_MAX_DELAY_MS = 1000;    // 1 second max delay
    private static final int DEFAULT_MAX_RETRIES = 3;         // Default max retries

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
        final Backoff backoff = Backoff.exponential(baseDelayMs, maxDelayMs)
                .withMaxAttempts(maxRetries);
        int attempt = 1;

        while (true) {
            try {
                task.run();
                return true; // Success
            } catch (Exception e) {
                long delayMillis = backoff.nextDelayMillis(attempt++);
                if (delayMillis < 0) {
                    log.error("Max retries ({}) reached. Last exception: {}", maxRetries, e.getMessage(), e);
                    return false;
                }

                log.warn("Retry attempt {} failed. Retrying in {} ms...", attempt - 1, delayMillis);

                try {
                    Thread.sleep(delayMillis);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    log.error("Retry thread interrupted", ie);
                    return false;
                }
            }
        }
    }

}
