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

import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;

import java.util.List;
import java.util.Optional;

/**
 * Strategy for determining how long to wait before retrying
 */
public interface RetryStrategy {
    List<Integer> DEFAULT_RETRY_ATTEMPT_SLEEP_TIME = List.of(1, 2, 5, 10, 20, 40);
    List<Integer> DEFAULT_RATE_LIMIT_RETRY_SLEEP_TIME = List.of(5, 10, 30, 60, 120, 300);
    int SLEEP_TIME_MULTIPLIER_MS = 1000;
    int MAX_RETRIES = DEFAULT_RETRY_ATTEMPT_SLEEP_TIME.size();

    /**
     * Calculate sleep time in milliseconds before next retry
     *
     * @param ex         The exception that triggered the retry
     * @param retryCount Current retry attempt (0-based)
     * @return Sleep time in milliseconds
     */
    long calculateSleepTime(Exception ex, int retryCount);

    /**
     * Get maximum number of retries allowed
     *
     * @return Maximum number of retries
     */
    int getMaxRetries();

    static Optional<HttpStatus> getStatusCode(final Exception ex) {
        if (ex instanceof HttpClientErrorException) {
            return Optional.of(((HttpClientErrorException) ex).getStatusCode());
        } else if (ex instanceof HttpServerErrorException) {
            return Optional.of(((HttpServerErrorException) ex).getStatusCode());
        }
        return Optional.empty();
    }
}
