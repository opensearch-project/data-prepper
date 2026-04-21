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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

class RetryAfterHeaderStrategyTest {

    @Test
    void constructor_WithCustomMaxRetries_InitializesSuccessfully() {
        final RetryAfterHeaderStrategy strategy = new RetryAfterHeaderStrategy(1);
        assertThat(strategy, notNullValue());
        assertThat(strategy.getMaxRetries(), equalTo(1));
    }

    @Test
    void constructor_WithCustomRateLimitSleepTime_InitializesSuccessfully() {
        final List<Integer> customSleepTime = Arrays.asList(10);
        final RetryAfterHeaderStrategy strategy = new RetryAfterHeaderStrategy(customSleepTime,null);
        assertThat(strategy, notNullValue());
        assertThat(strategy.getMaxRetries(), equalTo(1));
    }

    @Test
    void constructor_WithNullRateLimitSleepTime_UsesDefaultValues() {
        final RetryAfterHeaderStrategy strategy = new RetryAfterHeaderStrategy((List)null, (List)null);
        assertThat(strategy, notNullValue());
        assertThat(strategy.getMaxRetries(), equalTo(6));
    }

    @Test
    void getMaxRetries_ReturnsExpectedValue() {
        final RetryAfterHeaderStrategy retryAfterHeaderStrategy = new RetryAfterHeaderStrategy(1);
        assertThat(retryAfterHeaderStrategy.getMaxRetries(), equalTo(1));
    }

    @Test
    void calculateSleepTime_WithRetryAfterHeader_UsesHeaderValue() {
        final RetryAfterHeaderStrategy retryAfterHeaderStrategy = new RetryAfterHeaderStrategy(1);
        final HttpHeaders headers = new HttpHeaders();
        headers.set("retry-after", "15");
        final HttpClientErrorException exception = new HttpClientErrorException(
                HttpStatus.TOO_MANY_REQUESTS, "Too Many Requests", headers, null, null);

        final long sleepTime = retryAfterHeaderStrategy.calculateSleepTime(exception, 0);

        assertThat(sleepTime, equalTo(15000L));
    }

    @Test
    void calculateSleepTime_WithInvalidRetryAfterHeader_FallsBackToDefault() {
        final RetryAfterHeaderStrategy retryAfterHeaderStrategy = new RetryAfterHeaderStrategy(1);
        final HttpHeaders headers = new HttpHeaders();
        headers.set("retry-after", "invalid");
        final HttpClientErrorException exception = new HttpClientErrorException(
                HttpStatus.TOO_MANY_REQUESTS, "Too Many Requests", headers, null, null);

        final long sleepTime = retryAfterHeaderStrategy.calculateSleepTime(exception, 0);

        // Should fall back to default rate limit sleep time (5 seconds for first retry)
        assertThat(sleepTime, equalTo(5000L));
    }

    @Test
    void calculateSleepTime_WithMissingRetryAfterHeader_FallsBackToDefault() {
        final RetryAfterHeaderStrategy retryAfterHeaderStrategy = new RetryAfterHeaderStrategy(1);
        final HttpHeaders headers = new HttpHeaders();
        final HttpClientErrorException exception = new HttpClientErrorException(
                HttpStatus.TOO_MANY_REQUESTS, "Too Many Requests", headers, null, null);

        final long sleepTime = retryAfterHeaderStrategy.calculateSleepTime(exception, 0);

        assertThat(sleepTime, equalTo(5000L));
    }

    @Test
    void calculateSleepTime_WithNullHeaders_FallsBackToDefault() {
        final RetryAfterHeaderStrategy retryAfterHeaderStrategy = new RetryAfterHeaderStrategy(1);
        final HttpClientErrorException exception = new HttpClientErrorException(
                HttpStatus.TOO_MANY_REQUESTS, "Too Many Requests", null, null, null);

        final long sleepTime = retryAfterHeaderStrategy.calculateSleepTime(exception, 0);

        assertThat(sleepTime, equalTo(5000L));
    }

    @Test
    void calculateSleepTime_WithNonRateLimitError_UsesDefaultBackoff() {
        final RetryAfterHeaderStrategy retryAfterHeaderStrategy = new RetryAfterHeaderStrategy(1);
        final HttpServerErrorException exception = new HttpServerErrorException(
                HttpStatus.INTERNAL_SERVER_ERROR);

        final long sleepTime = retryAfterHeaderStrategy.calculateSleepTime(exception, 0);

        assertThat(sleepTime, equalTo(1000L));
    }

    @Test
    void calculateSleepTime_WithServerErrorAndRetryAfterHeader_IgnoresHeader() {
        final RetryAfterHeaderStrategy retryAfterHeaderStrategy = new RetryAfterHeaderStrategy(1);
        final HttpHeaders headers = new HttpHeaders();
        headers.set("retry-after", "15");
        final HttpServerErrorException exception = new HttpServerErrorException(
                HttpStatus.INTERNAL_SERVER_ERROR, "Internal Server Error", headers, null, null);

        final long sleepTime = retryAfterHeaderStrategy.calculateSleepTime(exception, 0);

        // Should use default backoff, not retry-after header
        assertThat(sleepTime, equalTo(1000L));
    }

    @Test
    void calculateSleepTime_WithCustomRateLimitSleepTime_UsesCustomValues() {
        final List<Integer> customSleepTime = Arrays.asList(10);
        final RetryAfterHeaderStrategy strategy = new RetryAfterHeaderStrategy(customSleepTime, null);
        final HttpClientErrorException exception = new HttpClientErrorException(
                HttpStatus.TOO_MANY_REQUESTS);

        final long sleepTime = strategy.calculateSleepTime(exception, 0);

        assertThat(sleepTime, equalTo(10000L));
    }

    @Test
    void calculateSleepTime_WithRetryCountExceedingList_ReturnsLastValue() {
        final RetryAfterHeaderStrategy retryAfterHeaderStrategy = new RetryAfterHeaderStrategy(1);
        final HttpServerErrorException exception = new HttpServerErrorException(
                HttpStatus.INTERNAL_SERVER_ERROR);

        final long sleepTime = retryAfterHeaderStrategy.calculateSleepTime(exception, 10);

        assertThat(sleepTime, equalTo(40000L));
    }

    @Test
    void calculateSleepTime_WithRateLimitAndExceedingCount_ReturnsLastValue() {
        final RetryAfterHeaderStrategy retryAfterHeaderStrategy = new RetryAfterHeaderStrategy(1);
        final HttpClientErrorException exception = new HttpClientErrorException(
                HttpStatus.TOO_MANY_REQUESTS);

        final long sleepTime = retryAfterHeaderStrategy.calculateSleepTime(exception, 10);

        assertThat(sleepTime, equalTo(300000L));
    }

    @Test
    void calculateSleepTime_WithGenericException_UsesDefaultBackoff() {
        final RetryAfterHeaderStrategy retryAfterHeaderStrategy = new RetryAfterHeaderStrategy(1);
        final Exception exception = new RuntimeException("Generic error");

        final long sleepTime = retryAfterHeaderStrategy.calculateSleepTime(exception, 0);

        assertThat(sleepTime, equalTo(1000L));
    }

    @ParameterizedTest
    @MethodSource("normalRetryArguments")
    void calculateSleepTime_WithNormalRetries_ReturnsExpectedTime(final int retryCount,
                                                                  final long expectedTimeMs) {
        final RetryAfterHeaderStrategy retryAfterHeaderStrategy = new RetryAfterHeaderStrategy(1);
        final HttpServerErrorException exception = new HttpServerErrorException(
                HttpStatus.INTERNAL_SERVER_ERROR);

        final long sleepTime = retryAfterHeaderStrategy.calculateSleepTime(exception, retryCount);

        assertThat(sleepTime, equalTo(expectedTimeMs));
    }

    @ParameterizedTest
    @MethodSource("rateLimitRetryArguments")
    void calculateSleepTime_WithRateLimitError_ReturnsExpectedTime(final int retryCount,
                                                                   final long expectedTimeMs) {
        final RetryAfterHeaderStrategy retryAfterHeaderStrategy = new RetryAfterHeaderStrategy(1);
        final HttpClientErrorException exception = new HttpClientErrorException(
                HttpStatus.TOO_MANY_REQUESTS);

        final long sleepTime = retryAfterHeaderStrategy.calculateSleepTime(exception, retryCount);

        assertThat(sleepTime, equalTo(expectedTimeMs));
    }

    @Test
    void calculateSleepTime_WithRetryAfterHeaderOnSecondRetry_UsesHeaderValue() {
        final RetryAfterHeaderStrategy retryAfterHeaderStrategy = new RetryAfterHeaderStrategy(1);
        final HttpHeaders headers = new HttpHeaders();
        headers.set("retry-after", "45");
        final HttpClientErrorException exception = new HttpClientErrorException(
                HttpStatus.TOO_MANY_REQUESTS, "Too Many Requests", headers, null, null);

        final long sleepTime = retryAfterHeaderStrategy.calculateSleepTime(exception, 3);

        assertThat(sleepTime, equalTo(45000L));
    }

    @Test
    void calculateSleepTime_WithZeroRetryAfterHeader_UsesHeaderValue() {
        final RetryAfterHeaderStrategy retryAfterHeaderStrategy = new RetryAfterHeaderStrategy(1);
        final HttpHeaders headers = new HttpHeaders();
        headers.set("retry-after", "0");
        final HttpClientErrorException exception = new HttpClientErrorException(
                HttpStatus.TOO_MANY_REQUESTS, "Too Many Requests", headers, null, null);

        final long sleepTime = retryAfterHeaderStrategy.calculateSleepTime(exception, 0);

        assertThat(sleepTime, equalTo(1000L));
    }

    @Test
    void calculateSleepTime_WithLargeRetryAfterHeader_UsesHeaderValue() {
        final RetryAfterHeaderStrategy retryAfterHeaderStrategy = new RetryAfterHeaderStrategy(1);
        final HttpHeaders headers = new HttpHeaders();
        headers.set("retry-after", "600");
        final HttpClientErrorException exception = new HttpClientErrorException(
                HttpStatus.TOO_MANY_REQUESTS, "Too Many Requests", headers, null, null);

        final long sleepTime = retryAfterHeaderStrategy.calculateSleepTime(exception, 0);

        assertThat(sleepTime, equalTo(600000L));
    }

    @Test
    void calculateSleepTime_WithEmptyRetryAfterHeader_FallsBackToDefault() {
        final RetryAfterHeaderStrategy retryAfterHeaderStrategy = new RetryAfterHeaderStrategy(1);
        final HttpHeaders headers = new HttpHeaders();
        headers.set("retry-after", "");
        final HttpClientErrorException exception = new HttpClientErrorException(
                HttpStatus.TOO_MANY_REQUESTS, "Too Many Requests", headers, null, null);

        final long sleepTime = retryAfterHeaderStrategy.calculateSleepTime(exception, 0);

        assertThat(sleepTime, equalTo(5000L));
    }

    @Test
    void calculateSleepTime_WithSequentialRetries_IncreasesBackoffTime() {
        final RetryAfterHeaderStrategy retryAfterHeaderStrategy = new RetryAfterHeaderStrategy(1);
        final HttpServerErrorException exception = new HttpServerErrorException(
                HttpStatus.INTERNAL_SERVER_ERROR);

        final long sleepTime0 = retryAfterHeaderStrategy.calculateSleepTime(exception, 0);
        final long sleepTime1 = retryAfterHeaderStrategy.calculateSleepTime(exception, 1);
        final long sleepTime2 = retryAfterHeaderStrategy.calculateSleepTime(exception, 2);

        assertThat(sleepTime0, equalTo(1000L));
        assertThat(sleepTime1, equalTo(2000L));
        assertThat(sleepTime2, equalTo(5000L));
    }

    private static Stream<Arguments> normalRetryArguments() {
        return Stream.of(
                Arguments.of(0, 1000L),
                Arguments.of(1, 2000L),
                Arguments.of(2, 5000L),
                Arguments.of(3, 10000L),
                Arguments.of(4, 20000L),
                Arguments.of(5, 40000L));
    }

    private static Stream<Arguments> rateLimitRetryArguments() {
        return Stream.of(
                Arguments.of(0, 5000L),
                Arguments.of(1, 10000L),
                Arguments.of(2, 30000L),
                Arguments.of(3, 60000L),
                Arguments.of(4, 120000L),
                Arguments.of(5, 300000L));
    }

    @Test
    void calculateSleepTime_WithRetryAfterHeaderAsInvalidHttpDate_ShouldFallBackToDefault() {
        final RetryAfterHeaderStrategy retryAfterHeaderStrategy = new RetryAfterHeaderStrategy(1);
        final HttpHeaders headers = new HttpHeaders();
        headers.set("retry-after", "Invalid Date Format");

        final HttpClientErrorException exception = new HttpClientErrorException(
                HttpStatus.TOO_MANY_REQUESTS, "Too Many Requests", headers, null, null);

        final long sleepTime = retryAfterHeaderStrategy.calculateSleepTime(exception, 0);

        // Should fall back to default rate limit sleep time
        assertThat(sleepTime, equalTo(5000L));
    }

    @Test
    void calculateSleepTime_WithXRateLimitRemainingZero_ShouldCalculateWaitTime() {
        final RetryAfterHeaderStrategy retryAfterHeaderStrategy = new RetryAfterHeaderStrategy(1);
        final HttpHeaders headers = new HttpHeaders();
        headers.set("X-RateLimit-Remaining", "0");
        final long resetTime = Instant.now().getEpochSecond() + 300;
        headers.set("X-RateLimit-Reset", String.valueOf(resetTime));

        final HttpClientErrorException exception = new HttpClientErrorException(
                HttpStatus.TOO_MANY_REQUESTS, "Rate limit exceeded", headers, null, null);

        final long sleepTime = retryAfterHeaderStrategy.calculateSleepTime(exception, 0);

        assertThat(sleepTime, equalTo(301000L));
    }

    @Test
    void calculateSleepTime_WithXRateLimitResetInPast_ShouldReturnMinimum() {
        final RetryAfterHeaderStrategy retryAfterHeaderStrategy = new RetryAfterHeaderStrategy(1);
        final HttpHeaders headers = new HttpHeaders();
        headers.set("X-RateLimit-Remaining", "0");
        final long pastResetTime = Instant.now().getEpochSecond() - 60;
        headers.set("X-RateLimit-Reset", String.valueOf(pastResetTime));

        final HttpClientErrorException exception = new HttpClientErrorException(
                HttpStatus.FORBIDDEN, "Rate limit expired", headers, null, null);

        final long sleepTime = retryAfterHeaderStrategy.calculateSleepTime(exception, 0);

        assertThat(sleepTime, equalTo(1000L));
    }

    @Test
    void calculateSleepTime_WithXRateLimitButRemainingNotZero_ShouldIgnoreHeaders() {
        final RetryAfterHeaderStrategy retryAfterHeaderStrategy = new RetryAfterHeaderStrategy(1);
        final HttpHeaders headers = new HttpHeaders();
        headers.set("X-RateLimit-Remaining", "10");
        final long resetTime = Instant.now().getEpochSecond() + 300;
        headers.set("X-RateLimit-Reset", String.valueOf(resetTime));

        final HttpClientErrorException exception = new HttpClientErrorException(
                HttpStatus.TOO_MANY_REQUESTS, "Too Many Request", headers, null, null);

        final long sleepTime = retryAfterHeaderStrategy.calculateSleepTime(exception, 0);

        assertThat(sleepTime, equalTo(5000L));
    }

    @Test
    void calculateSleepTime_WithXRateLimitResetBlank_ShouldIgnoreHeaders() {
        final RetryAfterHeaderStrategy retryAfterHeaderStrategy = new RetryAfterHeaderStrategy(1);
        final HttpHeaders headers = new HttpHeaders();
        headers.set("X-RateLimit-Remaining", "0");
        headers.set("X-RateLimit-Reset", "");

        final HttpClientErrorException exception = new HttpClientErrorException(
                HttpStatus.TOO_MANY_REQUESTS, "Too Many Request", headers, null, null);

        final long sleepTime = retryAfterHeaderStrategy.calculateSleepTime(exception, 0);

        assertThat(sleepTime, equalTo(5000L));
    }

    @Test
    void calculateSleepTime_WithXRateLimitResetInvalidNumber_ShouldIgnoreHeaders() {
        final RetryAfterHeaderStrategy retryAfterHeaderStrategy = new RetryAfterHeaderStrategy(1);
        final HttpHeaders headers = new HttpHeaders();
        headers.set("X-RateLimit-Remaining", "0");
        headers.set("X-RateLimit-Reset", "invalid-number");

        final HttpClientErrorException exception = new HttpClientErrorException(
                HttpStatus.TOO_MANY_REQUESTS, "Too Many Request", headers, null, null);

        final long sleepTime = retryAfterHeaderStrategy.calculateSleepTime(exception, 0);

        assertThat(sleepTime, equalTo(5000L));
    }

    @Test
    void calculateSleepTime_WithBothRetryAfterAndXRateLimit_ShouldPreferRetryAfter() {
        final RetryAfterHeaderStrategy retryAfterHeaderStrategy = new RetryAfterHeaderStrategy(1);
        final HttpHeaders headers = new HttpHeaders();
        headers.set("Retry-After", "60");
        headers.set("X-RateLimit-Remaining", "0");
        final long resetTime = Instant.now().getEpochSecond() + 300;
        headers.set("X-RateLimit-Reset", String.valueOf(resetTime));

        final HttpClientErrorException exception = new HttpClientErrorException(
                HttpStatus.TOO_MANY_REQUESTS, "Too Many Requests", headers, null, null);

        final long sleepTime = retryAfterHeaderStrategy.calculateSleepTime(exception, 0);

        assertThat(sleepTime, equalTo(60000L));
    }

    @Test
    void calculateSleepTime_WithXRateLimitResetOneSecondAway_ShouldReturnMinimum() {
        final RetryAfterHeaderStrategy retryAfterHeaderStrategy = new RetryAfterHeaderStrategy(1);
        final HttpHeaders headers = new HttpHeaders();
        headers.set("X-RateLimit-Remaining", "0");
        final long resetTime = Instant.now().getEpochSecond() + 1;
        headers.set("X-RateLimit-Reset", String.valueOf(resetTime));

        final HttpClientErrorException exception = new HttpClientErrorException(
                HttpStatus.TOO_MANY_REQUESTS, "Rate limit exceeded", headers, null, null);

        final long sleepTime = retryAfterHeaderStrategy.calculateSleepTime(exception, 0);

        assertThat(sleepTime, equalTo(2000L));
    }

    @Test
    void calculateSleepTime_WithCustomRateLimitHeadersCaseSensitive_PrefersRetryAfterHeader() {
        final RetryAfterHeaderStrategy strategy = new RetryAfterHeaderStrategy(
                "X-Custom-Rate-Limit-Remaining",
                "X-Custom-Rate-Limit-Reset"
        );

        final HttpHeaders headers = new HttpHeaders();
        headers.set("X-Custom-Retry-After", "50");
        headers.set("X-Custom-Rate-Limit-Remaining", "0");
        final long resetTime = Instant.now().getEpochSecond() + 10;
        headers.set("X-Custom-Rate-Limit-Reset", String.valueOf(resetTime));

        final HttpClientErrorException exception = new HttpClientErrorException(
                HttpStatus.TOO_MANY_REQUESTS, "Too Many Requests", headers, null, null);

        final long sleepTime = strategy.calculateSleepTime(exception, 0);

        assertThat(sleepTime, equalTo(11000L));
    }

    @Test
    void calculateSleepTime_WithCustomRateLimitHeadersCaseInsensitive_PrefersRetryAfterHeader() {
        final RetryAfterHeaderStrategy strategy = new RetryAfterHeaderStrategy(
                "x-custom-rate-limit-remaining",
                "x-custom-rate-limit-reset"
        );

        final HttpHeaders headers = new HttpHeaders();
        headers.set("X-Custom-Retry-After", "50");
        headers.set("X-Custom-Rate-Limit-Remaining", "0");
        final long resetTime = Instant.now().getEpochSecond() + 50;
        headers.set("X-Custom-Rate-Limit-Reset", String.valueOf(resetTime));

        final HttpClientErrorException exception = new HttpClientErrorException(
                HttpStatus.TOO_MANY_REQUESTS, "Too Many Requests", headers, null, null);

        final long sleepTime = strategy.calculateSleepTime(exception, 0);

        assertThat(sleepTime, equalTo(51000L));
    }

    @Test
    void calculateSleepTime_WithDefaultConstructor_UsesDefaultHeaders() {
        final RetryAfterHeaderStrategy strategy = new RetryAfterHeaderStrategy();

        final HttpHeaders headers = new HttpHeaders();
        headers.set("Retry-After", "40");
        final HttpClientErrorException exception = new HttpClientErrorException(
                HttpStatus.TOO_MANY_REQUESTS, "Too Many Requests", headers, null, null);

        final long sleepTime = strategy.calculateSleepTime(exception, 0);

        assertThat(sleepTime, equalTo(40000L));
    }
}
