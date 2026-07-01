/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe shared state for adaptive backoff across worker threads.
 * Tracks the last delay that resulted in a successful request and
 * decays back toward the initial delay after sustained first-attempt successes.
 */
public class AdaptiveBackoffState {

    private final AtomicLong lastSuccessfulDelay;
    private final AtomicInteger consecutiveFirstAttemptSuccesses;
    private final long initialDelayMs;
    private final int decayThreshold;

    public AdaptiveBackoffState(final long initialDelayMs, final int decayThreshold) {
        this.initialDelayMs = initialDelayMs;
        this.decayThreshold = decayThreshold;
        this.lastSuccessfulDelay = new AtomicLong(initialDelayMs);
        this.consecutiveFirstAttemptSuccesses = new AtomicInteger(0);
    }

    /**
     * Returns the delay to start retries at, based on previous batch outcomes.
     */
    public long getStartingDelay() {
        return lastSuccessfulDelay.get();
    }

    /**
     * Called when a batch required retries and eventually succeeded at the given delay.
     */
    public void recordRetrySuccess(final long delayMs) {
        lastSuccessfulDelay.set(delayMs);
        consecutiveFirstAttemptSuccesses.set(0);
    }

    /**
     * Called when a batch succeeds on the first attempt (no retries needed).
     * After decay_threshold consecutive first-attempt successes, halves the starting delay.
     */
    public void recordFirstAttemptSuccess() {
        int count = consecutiveFirstAttemptSuccesses.incrementAndGet();
        if (count >= decayThreshold) {
            long current = lastSuccessfulDelay.get();
            long decayed = Math.max(initialDelayMs, current / 2);
            lastSuccessfulDelay.compareAndSet(current, decayed);
            consecutiveFirstAttemptSuccesses.set(0);
        }
    }
}
