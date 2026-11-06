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
        return createExponentialBackoff(Duration.ofMillis(DEFAULT_MAX_DELAY_MILLIS));
    }

    /**
     * Creates an exponential backoff capped at {@code maximumDelay}.
     *
     * @param maximumDelay the maximum delay between retries. Must be non-null and at
     *                     least the backoff's initial delay of 20 seconds.
     * @return a configured {@link Backoff}
     * @throws IllegalArgumentException if {@code maximumDelay} is null or shorter than
     *                                  the initial delay
     */
    public static Backoff createExponentialBackoff(final Duration maximumDelay) {
        // Armeria rejects a maximum delay smaller than the initial delay.
        if (maximumDelay == null || maximumDelay.toMillis() < INITIAL_DELAY_MILLIS) {
            throw new IllegalArgumentException(
                    "maximumDelay must be at least " + Duration.ofMillis(INITIAL_DELAY_MILLIS)
                            + ", but was " + maximumDelay);
        }
        return Backoff.exponential(INITIAL_DELAY_MILLIS, maximumDelay.toMillis())
                .withJitter(JITTER_RATE)
                .withMaxAttempts(Integer.MAX_VALUE);
    }
}
