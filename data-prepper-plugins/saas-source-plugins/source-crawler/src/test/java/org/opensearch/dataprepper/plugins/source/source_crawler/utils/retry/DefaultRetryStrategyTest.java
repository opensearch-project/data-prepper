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
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

class DefaultRetryStrategyTest {

    @Test
    void constructor_WithCustomMaxRetries_InitializesSuccessfully() {
        final DefaultRetryStrategy strategy = new DefaultRetryStrategy(1);
        assertThat(strategy, notNullValue());
        assertThat(strategy.getMaxRetries(), equalTo(1));
    }

    @Test
    void constructor_WithCustomRateLimitSleepTime_InitializesSuccessfully() {
        final List<Integer> customSleepTime = Arrays.asList(10);
        final DefaultRetryStrategy strategy = new DefaultRetryStrategy(customSleepTime, null);
        assertThat(strategy, notNullValue());
        assertThat(strategy.getMaxRetries(), equalTo(1));
    }

    @Test
    void constructor_WithNullRateLimitSleepTime_UsesDefaultValues() {
        final DefaultRetryStrategy strategy = new DefaultRetryStrategy(null, null);
        assertThat(strategy, notNullValue());
        assertThat(strategy.getMaxRetries(), equalTo(6));
    }

    @Test
    void getMaxRetries_ReturnsExpectedValue() {
        final DefaultRetryStrategy defaultRetryStrategy = new DefaultRetryStrategy(1);
        assertThat(defaultRetryStrategy.getMaxRetries(), equalTo(1));
    }

    @ParameterizedTest
    @MethodSource("normalRetryArguments")
    void calculateSleepTime_WithNormalRetries_ReturnsExpectedTime(final int retryCount,
                                                                  final long expectedTimeMs) {
        final DefaultRetryStrategy defaultRetryStrategy = new DefaultRetryStrategy(1);
        final HttpServerErrorException exception = new HttpServerErrorException(
                HttpStatus.INTERNAL_SERVER_ERROR);

        final long sleepTime = defaultRetryStrategy.calculateSleepTime(exception, retryCount);

        assertThat(sleepTime, equalTo(expectedTimeMs));
    }

    @ParameterizedTest
    @MethodSource("rateLimitRetryArguments")
    void calculateSleepTime_WithRateLimitError_ReturnsExpectedTime(final int retryCount,
                                                                   final long expectedTimeMs) {
        final DefaultRetryStrategy defaultRetryStrategy = new DefaultRetryStrategy(1);
        final HttpClientErrorException exception = new HttpClientErrorException(
                HttpStatus.TOO_MANY_REQUESTS);

        final long sleepTime = defaultRetryStrategy.calculateSleepTime(exception, retryCount);

        assertThat(sleepTime, equalTo(expectedTimeMs));
    }

    @Test
    void calculateSleepTime_WithRetryCountExceedingList_ReturnsLastValue() {
        final DefaultRetryStrategy defaultRetryStrategy = new DefaultRetryStrategy(1);
        final HttpServerErrorException exception = new HttpServerErrorException(
                HttpStatus.INTERNAL_SERVER_ERROR);

        final long sleepTime = defaultRetryStrategy.calculateSleepTime(exception, 10);

        // Last value in default list is 40 seconds
        assertThat(sleepTime, equalTo(40000L));
    }

    @Test
    void calculateSleepTime_WithRateLimitAndExceedingCount_ReturnsLastValue() {
        final DefaultRetryStrategy defaultRetryStrategy = new DefaultRetryStrategy(1);
        final HttpClientErrorException exception = new HttpClientErrorException(
                HttpStatus.TOO_MANY_REQUESTS);

        final long sleepTime = defaultRetryStrategy.calculateSleepTime(exception, 10);

        // Last value in rate limit list is 300 seconds
        assertThat(sleepTime, equalTo(300000L));
    }

    @Test
    void calculateSleepTime_WithHttpClientErrorException_ReturnsCorrectTime() {
        final DefaultRetryStrategy defaultRetryStrategy = new DefaultRetryStrategy(1);
        final HttpClientErrorException exception = new HttpClientErrorException(
                HttpStatus.BAD_REQUEST);

        final long sleepTime = defaultRetryStrategy.calculateSleepTime(exception, 0);

        assertThat(sleepTime, equalTo(1000L));
    }

    @Test
    void calculateSleepTime_WithHttpServerErrorException_ReturnsCorrectTime() {
        final DefaultRetryStrategy defaultRetryStrategy = new DefaultRetryStrategy(1);
        final HttpServerErrorException exception = new HttpServerErrorException(
                HttpStatus.SERVICE_UNAVAILABLE);

        final long sleepTime = defaultRetryStrategy.calculateSleepTime(exception, 0);

        assertThat(sleepTime, equalTo(1000L));
    }

    @Test
    void calculateSleepTime_WithGenericException_ReturnsDefaultTime() {
        final DefaultRetryStrategy defaultRetryStrategy = new DefaultRetryStrategy(1);
        final Exception exception = new RuntimeException("Generic error");

        final long sleepTime = defaultRetryStrategy.calculateSleepTime(exception, 0);

        assertThat(sleepTime, equalTo(1000L));
    }

    @Test
    void calculateSleepTime_WithCustomRateLimitSleepTime_UsesCustomValues() {
        final List<Integer> customSleepTime = Arrays.asList(10);
        final DefaultRetryStrategy strategy = new DefaultRetryStrategy(customSleepTime, null);
        final HttpClientErrorException exception = new HttpClientErrorException(
                HttpStatus.TOO_MANY_REQUESTS);

        final long sleepTime1 = strategy.calculateSleepTime(exception, 0);

        assertThat(sleepTime1, equalTo(10000L));
    }

    @Test
    void calculateSleepTime_WithCustomRateLimitAndExceedingCount_ReturnsLastCustomValue() {
        final List<Integer> customSleepTime = Arrays.asList(10);
        final DefaultRetryStrategy strategy = new DefaultRetryStrategy(customSleepTime, null);
        final HttpClientErrorException exception = new HttpClientErrorException(
                HttpStatus.TOO_MANY_REQUESTS);

        final long sleepTime = strategy.calculateSleepTime(exception, 5);

        assertThat(sleepTime, equalTo(10000L));
    }

    @Test
    void calculateSleepTime_WithDifferentHttpStatusCodes_CalculatesCorrectly() {
        final DefaultRetryStrategy defaultRetryStrategy = new DefaultRetryStrategy(1);
        final HttpClientErrorException badRequestException = new HttpClientErrorException(
                HttpStatus.BAD_REQUEST);
        final HttpServerErrorException badGatewayException = new HttpServerErrorException(
                HttpStatus.BAD_GATEWAY);
        final HttpClientErrorException tooManyRequestsException = new HttpClientErrorException(
                HttpStatus.TOO_MANY_REQUESTS);

        final long sleepTime1 = defaultRetryStrategy.calculateSleepTime(badRequestException, 0);
        final long sleepTime2 = defaultRetryStrategy.calculateSleepTime(badGatewayException, 0);
        final long sleepTime3 = defaultRetryStrategy.calculateSleepTime(tooManyRequestsException,
                0);

        assertThat(sleepTime1, equalTo(1000L));
        assertThat(sleepTime2, equalTo(1000L));
        assertThat(sleepTime3, equalTo(5000L));
    }

    @Test
    void calculateSleepTime_WithSequentialRetries_IncreasesBackoffTime() {
        final DefaultRetryStrategy defaultRetryStrategy = new DefaultRetryStrategy(1);
        final HttpServerErrorException exception = new HttpServerErrorException(
                HttpStatus.INTERNAL_SERVER_ERROR);

        final long sleepTime0 = defaultRetryStrategy.calculateSleepTime(exception, 0);
        final long sleepTime1 = defaultRetryStrategy.calculateSleepTime(exception, 1);
        final long sleepTime2 = defaultRetryStrategy.calculateSleepTime(exception, 2);

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
}
