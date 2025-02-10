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
    private static final long INITIAL_DELAY_MILLIS = Duration.ofSeconds(20).toMillis();
    private static final long MAX_DELAY_MILLIS = Duration.ofMinutes(5).toMillis();
    private static final double JITTER_RATE = 0.20;

    private SqsBackoff() {}

    public static Backoff createExponentialBackoff() {
        return Backoff.exponential(INITIAL_DELAY_MILLIS, MAX_DELAY_MILLIS)
                .withJitter(JITTER_RATE)
                .withMaxAttempts(Integer.MAX_VALUE);
    }
}
