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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqsBackoffTest {

    @Test
    void testCreateExponentialBackoff() {
        final Backoff backoff = SqsBackoff.createExponentialBackoff();
        assertNotNull(backoff, "Backoff should not be null");
        final long firstDelay = backoff.nextDelayMillis(1);
        final long expectedBaseDelay = 20_000L;
        final double jitterRate = 0.20;
        final long minDelay = (long) (expectedBaseDelay * (1 - jitterRate));
        final long maxDelay = (long) (expectedBaseDelay * (1 + jitterRate));

        assertTrue(
                firstDelay >= minDelay && firstDelay <= maxDelay,
                String.format("First delay %dms should be between %dms and %dms",
                        firstDelay, minDelay, maxDelay)
        );
    }
}
