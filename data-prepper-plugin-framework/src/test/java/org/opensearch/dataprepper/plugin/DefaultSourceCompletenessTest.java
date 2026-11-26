/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */


/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.function.ToDoubleFunction;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class DefaultSourceCompletenessTest {
    private static final Instant NOW = Instant.parse("2026-11-04T19:05:00Z");

    @Mock
    private PluginMetrics pluginMetrics;

    private DefaultSourceCompleteness objectUnderTest;

    @BeforeEach
    void setUp() {
        objectUnderTest = new DefaultSourceCompleteness(pluginMetrics, Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @Test
    void constructor_registers_the_three_gauges_on_the_plugin_metrics() {
        verify(pluginMetrics).gauge(eq(DefaultSourceCompleteness.COMPLETE_UP_TO), eq(objectUnderTest), any());
        verify(pluginMetrics).gauge(eq(DefaultSourceCompleteness.FRESHNESS_LAG), eq(objectUnderTest), any());
        verify(pluginMetrics).gauge(eq(DefaultSourceCompleteness.REPORTING_PARTITIONS), eq(objectUnderTest), any());
    }

    @Test
    void no_reporting_partitions_means_empty_completeUpTo_and_zero_gauges() {
        assertThat(objectUnderTest.completeUpTo(), equalTo(Optional.empty()));
        assertThat(objectUnderTest.freshnessLagSeconds(), equalTo(0.0));
        assertThat(gaugeValue(DefaultSourceCompleteness.COMPLETE_UP_TO), equalTo(0.0));
        assertThat(gaugeValue(DefaultSourceCompleteness.REPORTING_PARTITIONS), equalTo(0.0));
    }

    @Test
    void completeUpTo_is_the_oldest_partition() {
        objectUnderTest.update("orders-0", NOW.minusSeconds(288));
        objectUnderTest.update("orders-1", NOW.minusSeconds(20));

        assertThat(objectUnderTest.completeUpTo(), equalTo(Optional.of(NOW.minusSeconds(288))));
        assertThat(objectUnderTest.freshnessLagSeconds(), equalTo(288.0));
        assertThat(gaugeValue(DefaultSourceCompleteness.COMPLETE_UP_TO),
                equalTo((double) NOW.minusSeconds(288).toEpochMilli()));
        assertThat(gaugeValue(DefaultSourceCompleteness.FRESHNESS_LAG), equalTo(288.0));
        assertThat(gaugeValue(DefaultSourceCompleteness.REPORTING_PARTITIONS), equalTo(2.0));
    }

    @Test
    void update_is_monotonic_per_partition() {
        objectUnderTest.update("orders-0", NOW.minusSeconds(60));
        objectUnderTest.update("orders-0", NOW.minusSeconds(120));

        assertThat(objectUnderTest.completeUpTo(), equalTo(Optional.of(NOW.minusSeconds(60))));
    }

    @Test
    void caught_up_partition_counts_as_now_and_does_not_hold_the_source_back() {
        objectUnderTest.update("orders-0", NOW.minusSeconds(30));
        objectUnderTest.markCaughtUp("orders-1");

        assertThat(objectUnderTest.completeUpTo(), equalTo(Optional.of(NOW.minusSeconds(30))));

        objectUnderTest.markCaughtUp("orders-0");

        assertThat(objectUnderTest.completeUpTo(), equalTo(Optional.of(NOW)));
        assertThat(objectUnderTest.freshnessLagSeconds(), equalTo(0.0));
    }

    @Test
    void update_after_caught_up_leaves_the_caught_up_state() {
        objectUnderTest.markCaughtUp("orders-0");
        objectUnderTest.update("orders-0", NOW.minusSeconds(45));

        assertThat(objectUnderTest.completeUpTo(), equalTo(Optional.of(NOW.minusSeconds(45))));
    }

    @Test
    void released_partition_leaves_the_aggregate() {
        objectUnderTest.update("orders-0", NOW.minusSeconds(600));
        objectUnderTest.update("orders-1", NOW.minusSeconds(5));

        objectUnderTest.release("orders-0");

        assertThat(objectUnderTest.completeUpTo(), equalTo(Optional.of(NOW.minusSeconds(5))));
        assertThat(gaugeValue(DefaultSourceCompleteness.REPORTING_PARTITIONS), equalTo(1.0));
    }

    @Test
    void freshness_lag_never_goes_negative() {
        objectUnderTest.update("orders-0", NOW.plusSeconds(10));

        assertThat(objectUnderTest.freshnessLagSeconds(), equalTo(0.0));
    }

    @Test
    void null_arguments_are_rejected() {
        assertThrows(NullPointerException.class, () -> objectUnderTest.update(null, NOW));
        assertThrows(NullPointerException.class, () -> objectUnderTest.update("orders-0", null));
        assertThrows(NullPointerException.class, () -> objectUnderTest.markCaughtUp(null));
        assertThrows(NullPointerException.class, () -> objectUnderTest.release(null));
    }

    @SuppressWarnings("unchecked")
    private double gaugeValue(final String name) {
        final ArgumentCaptor<ToDoubleFunction<DefaultSourceCompleteness>> captor =
                ArgumentCaptor.forClass(ToDoubleFunction.class);
        verify(pluginMetrics).gauge(eq(name), eq(objectUnderTest), captor.capture());
        return captor.getValue().applyAsDouble(objectUnderTest);
    }
}
