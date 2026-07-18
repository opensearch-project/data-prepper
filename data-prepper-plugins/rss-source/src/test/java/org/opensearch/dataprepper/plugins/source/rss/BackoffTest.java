/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rss;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

class BackoffTest {

    @Test
    void nextDelay_grows_exponentially_and_is_capped() {
        final Backoff backoff = new Backoff(Duration.ofSeconds(1), Duration.ofSeconds(60), 2.0, 0.0);
        assertThat(backoff.nextDelayMillis(1), lessThanOrEqualTo(1000L));
        assertThat(backoff.nextDelayMillis(3), lessThanOrEqualTo(4000L));
        assertThat(backoff.nextDelayMillis(100), lessThanOrEqualTo(60000L));
    }

    @Test
    void nextDelay_at_least_base_with_no_jitter() {
        final Backoff backoff = new Backoff(Duration.ofSeconds(1), Duration.ofSeconds(60), 2.0, 0.0);
        assertThat(backoff.nextDelayMillis(1), greaterThanOrEqualTo(1000L));
    }

    @Test
    void jitter_stays_within_bounds() {
        final Backoff backoff = new Backoff(Duration.ofSeconds(1), Duration.ofSeconds(60), 2.0, 0.5);
        for (int i = 0; i < 50; i++) {
            final long delay = backoff.nextDelayMillis(1);
            // base 1000ms, up to +50% jitter -> [1000, 1500]
            assertThat(delay, greaterThanOrEqualTo(1000L));
            assertThat(delay, lessThanOrEqualTo(1500L));
        }
    }
}
