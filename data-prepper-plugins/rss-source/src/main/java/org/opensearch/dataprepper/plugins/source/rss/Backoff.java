/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.rss;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Exponential backoff with optional jitter, capped at a maximum. Follows the
 * OpenSearch source WorkerCommonUtils pattern (base * rate^(n-1) + jitter).
 */
class Backoff {

    private final long baseMillis;
    private final long maxMillis;
    private final double rate;
    private final double jitterFraction;

    Backoff(final Duration base, final Duration max, final double rate, final double jitterFraction) {
        this.baseMillis = base.toMillis();
        this.maxMillis = max.toMillis();
        this.rate = rate;
        this.jitterFraction = jitterFraction;
        if (baseMillis < 0 || maxMillis < 0) {
            throw new IllegalArgumentException("base and max durations must be non-negative");
        }
        if (baseMillis > maxMillis) {
            throw new IllegalArgumentException("base duration must not exceed max duration");
        }
        if (rate < 1) {
            throw new IllegalArgumentException("rate must be at least 1, but was " + rate);
        }
        if (jitterFraction < 0 || jitterFraction > 1) {
            throw new IllegalArgumentException("jitterFraction must be in [0, 1], but was " + jitterFraction);
        }
    }

    long nextDelayMillis(final int failureCount) {
        final int exponent = Math.max(0, Math.min(failureCount - 1, 30));
        final double raw = baseMillis * Math.pow(rate, exponent);
        long delay = (long) Math.min(raw, maxMillis);
        if (jitterFraction > 0) {
            final long jitter = (long) (delay * jitterFraction);
            if (jitter > 0) {
                delay += ThreadLocalRandom.current().nextLong(0, jitter + 1);
            }
        }
        return Math.min(delay, maxMillis);
    }
}
