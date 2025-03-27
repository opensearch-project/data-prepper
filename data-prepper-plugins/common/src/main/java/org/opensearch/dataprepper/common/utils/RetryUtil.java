/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.common.utils;

public class RetryUtil {
    private static final int MAX_RETRIES = 3;
    private static final long BASE_DELAY_MS = 100; // 100ms base delay for exponential backoff

    public static boolean retryWithBackoff(Runnable task) {
        int attempt = 0;
        while (attempt < MAX_RETRIES) {
            try {
                task.run();
                return true; // Success
            } catch (Exception e) {
                try {
                    Thread.sleep(BASE_DELAY_MS * (1L << attempt)); // Exponential backoff
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            attempt++;
        }
        return false; // Failed after retries
    }

}
