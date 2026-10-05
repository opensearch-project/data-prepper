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

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

    @Test
    void constructor_rejects_base_exceeding_max() {
        assertThrows(IllegalArgumentException.class,
                () -> new Backoff(Duration.ofSeconds(60), Duration.ofSeconds(1), 2.0, 0.0));
    }

    @Test
    void constructor_rejects_negative_duration() {
        assertThrows(IllegalArgumentException.class,
                () -> new Backoff(Duration.ofSeconds(-1), Duration.ofSeconds(60), 2.0, 0.0));
    }

    @Test
    void constructor_rejects_rate_below_one() {
        assertThrows(IllegalArgumentException.class,
                () -> new Backoff(Duration.ofSeconds(1), Duration.ofSeconds(60), 0.5, 0.0));
    }

    @Test
    void constructor_rejects_jitter_fraction_outside_unit_interval() {
        assertThrows(IllegalArgumentException.class,
                () -> new Backoff(Duration.ofSeconds(1), Duration.ofSeconds(60), 2.0, 1.5));
        assertThrows(IllegalArgumentException.class,
                () -> new Backoff(Duration.ofSeconds(1), Duration.ofSeconds(60), 2.0, -0.1));
    }

    @Test
    void constructor_rejects_null_durations_with_illegal_argument() {
        assertThrows(IllegalArgumentException.class,
                () -> new Backoff(null, Duration.ofSeconds(60), 2.0, 0.0));
        assertThrows(IllegalArgumentException.class,
                () -> new Backoff(Duration.ofSeconds(1), null, 2.0, 0.0));
    }
}
