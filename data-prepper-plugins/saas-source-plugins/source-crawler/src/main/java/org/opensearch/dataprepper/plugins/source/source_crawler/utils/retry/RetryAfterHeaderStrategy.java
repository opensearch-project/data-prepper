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
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;

/**
 * Retry strategy that respects retry-after header
 */
@Slf4j
public class RetryAfterHeaderStrategy implements RetryStrategy {
    private static final String RATE_LIMIT_REMAINING = "X-RateLimit-Remaining";
    private static final String RATE_LIMIT_RESET = "X-RateLimit-Reset";
    private static final String RETRY_AFTER = "Retry-After";

    private final List<Integer> retryAttemptSleepTime;
    private final List<Integer> rateLimitRetrySleepTime;

    /**
     * Constructor with default sleep times
     */
    public RetryAfterHeaderStrategy() {
        this.retryAttemptSleepTime = RetryStrategy.DEFAULT_RETRY_ATTEMPT_SLEEP_TIME;
        this.rateLimitRetrySleepTime = RetryStrategy.DEFAULT_RATE_LIMIT_RETRY_SLEEP_TIME;
    }

    /**
     * Constructor with Custom sleep times for rate limit retries
     *
     * @param rateLimitRetrySleepTime Custom sleep times for rate limit retries (in
     *                                seconds)
     */
    public RetryAfterHeaderStrategy(List<Integer> rateLimitRetrySleepTime) {
        this.retryAttemptSleepTime = RetryStrategy.DEFAULT_RETRY_ATTEMPT_SLEEP_TIME;
        this.rateLimitRetrySleepTime = rateLimitRetrySleepTime != null
                ? rateLimitRetrySleepTime
                : RetryStrategy.DEFAULT_RATE_LIMIT_RETRY_SLEEP_TIME;
    }

    @Override
    public long calculateSleepTime(Exception ex, int retryCount) {
        Optional<HttpStatus> statusCode = RetryStrategy.getStatusCode(ex);

        if (statusCode.isPresent() && isRateLimited(statusCode.get())) {
            final Optional<Integer> retryAfterSeconds = extractRetryAfterHeader(ex);
            if (retryAfterSeconds.isPresent()) {
                log.info("Using retry-after header value: {} seconds (attempt {}/{})",
                        retryAfterSeconds.get(), retryCount + 1, getMaxRetries());
                return retryAfterSeconds.get() * RetryStrategy.SLEEP_TIME_MULTIPLIER_MS;
            }
        }

        // Fallback to fixed backoff
        List<Integer> sleepTimes = (statusCode.isPresent() && isRateLimited(statusCode.get()))
                ? rateLimitRetrySleepTime
                : retryAttemptSleepTime;

        int sleepTimeSeconds = (retryCount < sleepTimes.size())
                ? sleepTimes.get(retryCount)
                : sleepTimes.get(sleepTimes.size() - 1);

        log.debug("Retrying in {} seconds (attempt {}/{})",
                sleepTimeSeconds, retryCount + 1, getMaxRetries());

        return sleepTimeSeconds * RetryStrategy.SLEEP_TIME_MULTIPLIER_MS;
    }

    private boolean isRateLimited(final HttpStatus status) {
        return status == HttpStatus.TOO_MANY_REQUESTS ||
                status == HttpStatus.FORBIDDEN ||
                status == HttpStatus.SERVICE_UNAVAILABLE;
    }

    private Optional<Integer> extractRetryAfterHeader(Exception ex) {
        try {
            HttpHeaders headers = null;
            if (ex instanceof HttpClientErrorException) {
                headers = ((HttpClientErrorException) ex).getResponseHeaders();
            } else if (ex instanceof HttpServerErrorException) {
                headers = ((HttpServerErrorException) ex).getResponseHeaders();
            }

            if (headers != null && headers.containsKey(RETRY_AFTER)) {
                String retryAfter = headers.getFirst(RETRY_AFTER);
                if (retryAfter != null) {
                    int seconds = Integer.parseInt(retryAfter);
                    return Optional.of(Math.max(seconds, 1));
                }
            }
            if (headers != null && headers.containsKey(RATE_LIMIT_REMAINING) && headers.containsKey(RATE_LIMIT_RESET)) {
                String xRateLimitRemaining = headers.getFirst(RATE_LIMIT_REMAINING);
                String resetEpoch = headers.getFirst(RATE_LIMIT_RESET);
                if (xRateLimitRemaining != null && xRateLimitRemaining.equals("0") && resetEpoch != null
                        && !resetEpoch.isBlank()) {
                    long resetSeconds = Long.parseLong(resetEpoch);
                    long nowSeconds = Instant.now().getEpochSecond();
                    long wait = resetSeconds - nowSeconds + 1;
                    return Optional.of((int) Math.max(wait, 1));
                }
            }
        } catch (NumberFormatException e) {
            log.warn(NOISY, "Failed to parse retry-after header: {}", e.getMessage());
        }
        return Optional.empty();
    }

}