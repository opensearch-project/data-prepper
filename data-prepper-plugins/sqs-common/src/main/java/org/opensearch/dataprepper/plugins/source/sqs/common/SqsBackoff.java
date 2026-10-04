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
import java.time.Duration;

public final class SqsBackoff {
    static final long INITIAL_DELAY_MILLIS = Duration.ofSeconds(20).toMillis();
    static final long DEFAULT_MAX_DELAY_MILLIS = Duration.ofMinutes(5).toMillis();
    static final double JITTER_RATE = 0.20;

    private SqsBackoff() {}

    /**
     * Creates an exponential backoff that uses the default maximum delay.
     *
     * @return a configured {@link Backoff}
     */
    public static Backoff createExponentialBackoff() {
        return createExponentialBackoff(null);
    }

    /**
     * Creates an exponential backoff capped at {@code maximumDelay}. If the value is
     * null, zero, negative, or shorter than the backoff's initial delay, the factory
     * falls back to the default maximum rather than throwing.
     *
     * @param maximumDelay the maximum delay between retries
     * @return a configured {@link Backoff}
     */
    public static Backoff createExponentialBackoff(final Duration maximumDelay) {
        final long maxDelayMillis = isValidMaximumDelay(maximumDelay) ? maximumDelay.toMillis() : DEFAULT_MAX_DELAY_MILLIS;
        return Backoff.exponential(INITIAL_DELAY_MILLIS, maxDelayMillis)
                .withJitter(JITTER_RATE)
                .withMaxAttempts(Integer.MAX_VALUE);
    }

    // Armeria rejects a max below the initial delay. Fall back to the default on null, zero,
    // negative, or sub-initial values so direct callers do not crash the pipeline.
    private static boolean isValidMaximumDelay(final Duration duration) {
        return duration != null && duration.toMillis() >= INITIAL_DELAY_MILLIS;
    }
}
