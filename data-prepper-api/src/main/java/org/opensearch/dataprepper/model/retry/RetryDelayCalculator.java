/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.retry;

import org.apache.commons.lang3.ObjectUtils;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Calculates the retry delay that a server sends to clients when it rejects requests, independent of
 * the protocol. The delay starts at the minimum, doubles on consecutive rejections up to the maximum,
 * and falls back to the minimum once more time than the current delay has passed since the last
 * rejection.
 * <p>
 * Protocol-specific callers convert the returned {@link Duration}, for example into a gRPC
 * {@code RetryInfo} or an HTTP {@code Retry-After} header.
 *
 * @since 2.18
 */
public class RetryDelayCalculator {
    public static final Duration DEFAULT_MINIMUM_DELAY = Duration.ofMillis(100);
    public static final Duration DEFAULT_MAXIMUM_DELAY = Duration.ofMillis(2000);
    private static final int BACKOFF_MULTIPLIER = 2;

    private final Duration minimumDelay;
    private final Duration maximumDelay;
    private final Clock clock;

    private final AtomicReference<Instant> lastTimeCalled;
    private final AtomicReference<Duration> nextDelay;

    public RetryDelayCalculator(final Duration minimumDelay, final Duration maximumDelay) {
        this(minimumDelay, maximumDelay, Clock.systemUTC());
    }

    RetryDelayCalculator(final Duration minimumDelay, final Duration maximumDelay, final Clock clock) {
        this.minimumDelay = Objects.requireNonNull(minimumDelay, "minimumDelay must not be null");
        this.maximumDelay = Objects.requireNonNull(maximumDelay, "maximumDelay must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        // Create a cushion so that the calculator treats a first quick rejection (after startup) as a normal request (e.g. does not calculate a backoff)
        this.lastTimeCalled = new AtomicReference<>(clock.instant().minus(maximumDelay));
        this.nextDelay = new AtomicReference<>(minimumDelay);
    }

    /**
     * Returns the retry delay for the current rejection and records the time of the call.
     *
     * @return the retry delay to send to the client
     */
    public Duration nextDelay() {
        final Instant now = clock.instant();
        // Is the last time we got called longer ago than the next delay?
        if (lastTimeCalled.getAndSet(now).isBefore(now.minus(nextDelay.get()))) {
            // Use minimum delay and reset the saved delay
            nextDelay.set(minimumDelay);
            return minimumDelay;
        }
        return nextDelay.getAndUpdate(delay -> ObjectUtils.min(maximumDelay, delay.multipliedBy(BACKOFF_MULTIPLIER)));
    }

    /**
     * Converts a retry delay into the value of an HTTP {@code Retry-After} header: whole seconds,
     * rounded up, and at least 1.
     *
     * @param delay the retry delay
     * @return the delay in whole seconds for the {@code Retry-After} header
     */
    public static long toRetryAfterSeconds(final Duration delay) {
        // TODO: Use Math.ceilDiv(delay.toNanos(), 1_000_000_000L) once Data Prepper requires Java 18 or later.
        final long seconds = delay.getSeconds() + (delay.getNano() > 0 ? 1 : 0);
        return Math.max(1, seconds);
    }
}
