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

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Default retry strategy with fixed backoff times
 */
@Slf4j
public class DefaultRetryStrategy implements RetryStrategy {
    private static final List<HttpStatus> DEFAULT_RATE_LIMIT_STATUS_CODES = Arrays.asList(HttpStatus.TOO_MANY_REQUESTS);

    private final List<Integer> retryAttemptSleepTime;
    private final List<Integer> rateLimitRetrySleepTime;
    private final List<HttpStatus> rateLimitStatusCodes;
    private final int maxRetries;

    /**
     * Constructor with default sleep times
     */
    public DefaultRetryStrategy() {
        this.retryAttemptSleepTime = RetryStrategy.DEFAULT_RETRY_ATTEMPT_SLEEP_TIME;
        this.rateLimitRetrySleepTime = RetryStrategy.DEFAULT_RATE_LIMIT_RETRY_SLEEP_TIME;
        this.rateLimitStatusCodes = DEFAULT_RATE_LIMIT_STATUS_CODES;
        this.maxRetries = RetryStrategy.MAX_RETRIES;
    }

    /**
     * Constructor with custom max retries
     *
     * @param maxRetries Maximum number of retries
     */
    public DefaultRetryStrategy(final int maxRetries) {
        this.retryAttemptSleepTime = RetryStrategy.DEFAULT_RETRY_ATTEMPT_SLEEP_TIME;
        this.rateLimitRetrySleepTime = RetryStrategy.DEFAULT_RATE_LIMIT_RETRY_SLEEP_TIME;
        this.rateLimitStatusCodes = DEFAULT_RATE_LIMIT_STATUS_CODES;
        this.maxRetries = maxRetries;
    }

    /**
     * Constructor with Custom sleep times for rate limit retries and custom rate limit status codes
     *
     * @param rateLimitRetrySleepTime Custom sleep times for rate limit retries (in
     *                                seconds)
     * @param rateLimitStatusCodes List of status codes that are considered rate limited
     */
    public DefaultRetryStrategy(List<Integer> rateLimitRetrySleepTime, List<HttpStatus> rateLimitStatusCodes) {
        this.retryAttemptSleepTime = RetryStrategy.DEFAULT_RETRY_ATTEMPT_SLEEP_TIME;
        this.rateLimitRetrySleepTime = rateLimitRetrySleepTime != null
                ? rateLimitRetrySleepTime
                : RetryStrategy.DEFAULT_RATE_LIMIT_RETRY_SLEEP_TIME;
        this.rateLimitStatusCodes = rateLimitStatusCodes != null
                ? rateLimitStatusCodes
                : DEFAULT_RATE_LIMIT_STATUS_CODES;
        this.maxRetries = this.rateLimitRetrySleepTime.size();
    }

    @Override
    public long calculateSleepTime(Exception ex, int retryCount) {
        Optional<HttpStatus> statusCode = RetryStrategy.getStatusCode(ex);

        List<Integer> sleepTimes = (statusCode.isPresent() && rateLimitStatusCodes.contains(statusCode.get()))
                ? rateLimitRetrySleepTime
                : retryAttemptSleepTime;

        int sleepTimeSeconds = (retryCount < sleepTimes.size())
                ? sleepTimes.get(retryCount)
                : sleepTimes.get(sleepTimes.size() - 1);

        log.debug("Retrying in {} seconds (attempt {}/{})",
                sleepTimeSeconds, retryCount + 1, getMaxRetries());

        return sleepTimeSeconds * RetryStrategy.SLEEP_TIME_MULTIPLIER_MS;
    }

    @Override
    public int getMaxRetries() {
        return maxRetries;
    }

}