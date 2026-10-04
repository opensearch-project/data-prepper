/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.sqs.common;

import com.linecorp.armeria.client.retry.Backoff;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

class SqsBackoffTest {

    @Test
    void testCreateExponentialBackoff() {
        final Backoff backoff = SqsBackoff.createExponentialBackoff();
        assertThat("Backoff should not be null", backoff, is(notNullValue()));

        final long firstDelay = backoff.nextDelayMillis(1);
        final long minDelay = (long) (SqsBackoff.INITIAL_DELAY_MILLIS * (1 - SqsBackoff.JITTER_RATE));
        final long maxDelay = (long) (SqsBackoff.INITIAL_DELAY_MILLIS * (1 + SqsBackoff.JITTER_RATE));
        assertThat("First delay should be at or above the default initial delay lower bound",
                firstDelay, is(greaterThanOrEqualTo(minDelay)));
        assertThat("First delay should be at or below the default initial delay upper bound",
                firstDelay, is(lessThanOrEqualTo(maxDelay)));
    }

    @Test
    void testCreateExponentialBackoff_capsAtConfiguredMaximumDelay() {
        final Duration maximumDelay = Duration.ofMinutes(1);
        final Backoff backoff = SqsBackoff.createExponentialBackoff(maximumDelay);
        assertThat("Backoff should not be null", backoff, is(notNullValue()));

        // After many attempts the exponential delay should be clamped to the configured maximum.
        final long maxDelayCeiling = (long) (maximumDelay.toMillis() * (1 + SqsBackoff.JITTER_RATE));
        final long delayAtHighAttempt = backoff.nextDelayMillis(50);
        assertThat("Delay at high attempt should be at or below the configured maximum ceiling",
                delayAtHighAttempt, is(lessThanOrEqualTo(maxDelayCeiling)));
    }

    @ParameterizedTest(name = "falls back to default when maximum delay is {0}")
    @MethodSource("invalidMaximumDelays")
    void testCreateExponentialBackoff_fallsBackOnInvalidMaximumDelay(final Duration invalidMaximumDelay) {
        // Armeria rejects a maximum delay smaller than the initial delay. The factory must shield
        // callers that bypass config validation by falling back to the default instead of throwing.
        final Backoff backoff = SqsBackoff.createExponentialBackoff(invalidMaximumDelay);
        assertThat(String.format("Backoff should not be null for invalid maximum delay %s", invalidMaximumDelay),
                backoff, is(notNullValue()));

        final long firstDelay = backoff.nextDelayMillis(1);
        final long minDelay = (long) (SqsBackoff.INITIAL_DELAY_MILLIS * (1 - SqsBackoff.JITTER_RATE));
        final long maxDelay = (long) (SqsBackoff.INITIAL_DELAY_MILLIS * (1 + SqsBackoff.JITTER_RATE));
        assertThat(String.format("First delay for input %s should be at or above the default lower bound", invalidMaximumDelay),
                firstDelay, is(greaterThanOrEqualTo(minDelay)));
        assertThat(String.format("First delay for input %s should be at or below the default upper bound", invalidMaximumDelay),
                firstDelay, is(lessThanOrEqualTo(maxDelay)));

        final long defaultMaxCeiling = (long) (SqsBackoff.DEFAULT_MAX_DELAY_MILLIS * (1 + SqsBackoff.JITTER_RATE));
        final long delayAtHighAttempt = backoff.nextDelayMillis(50);
        assertThat(String.format("Delay at high attempt for input %s should be at or below the default ceiling", invalidMaximumDelay),
                delayAtHighAttempt, is(lessThanOrEqualTo(defaultMaxCeiling)));
    }

    private static Stream<Arguments> invalidMaximumDelays() {
        return Stream.of(
                Arguments.of((Duration) null),
                Arguments.of(Duration.ZERO),
                Arguments.of(Duration.ofSeconds(-1)),
                Arguments.of(Duration.ofSeconds(5))
        );
    }
}
