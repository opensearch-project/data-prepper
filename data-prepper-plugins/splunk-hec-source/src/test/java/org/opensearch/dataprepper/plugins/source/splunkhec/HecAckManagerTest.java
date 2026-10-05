/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.splunkhec;

import io.micrometer.core.instrument.Counter;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToDoubleFunction;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class HecAckManagerTest {

    private static final String CHANNEL = "test-channel-123";
    private static final Duration ACK_EXPIRY = Duration.ofSeconds(300);

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Counter ackRequestsCounter;

    @Mock
    private Counter ackConfirmedCounter;

    @Mock
    private Counter ackExpiredCounter;

    private HecAckManager ackManager;

    @BeforeEach
    void setUp() {
        when(pluginMetrics.counter(HecAckManager.ACK_REQUESTS_TOTAL)).thenReturn(ackRequestsCounter);
        when(pluginMetrics.counter(HecAckManager.ACK_CONFIRMED_TOTAL)).thenReturn(ackConfirmedCounter);
        when(pluginMetrics.counter(HecAckManager.ACK_EXPIRED_TOTAL)).thenReturn(ackExpiredCounter);
        when(pluginMetrics.gauge(eq(HecAckManager.ACK_PENDING), any(AtomicLong.class), any(ToDoubleFunction.class)))
                .thenReturn(new AtomicLong(0));
        ackManager = new HecAckManager(ACK_EXPIRY, pluginMetrics);
    }

    @Test
    void constructor_with_null_ackExpiry_throws_NullPointerException() {
        assertThrows(NullPointerException.class, () -> new HecAckManager(null, pluginMetrics));
    }

    @Test
    void constructor_with_null_pluginMetrics_throws_NullPointerException() {
        assertThrows(NullPointerException.class, () -> new HecAckManager(ACK_EXPIRY, null));
    }

    @Test
    void createAck_returns_monotonically_increasing_ids_for_same_channel() {
        final long ack1 = ackManager.createAck(CHANNEL);
        final long ack2 = ackManager.createAck(CHANNEL);
        final long ack3 = ackManager.createAck(CHANNEL);

        assertThat(ack1, equalTo(0L));
        assertThat(ack2, equalTo(1L));
        assertThat(ack3, equalTo(2L));
    }

    @Test
    void createAck_returns_separate_ids_for_different_channels() {
        final long ack1 = ackManager.createAck("channel-a");
        final long ack2 = ackManager.createAck("channel-b");

        assertThat(ack1, equalTo(0L));
        assertThat(ack2, equalTo(0L));
    }

    @Test
    void createAck_with_null_channel_throws_NullPointerException() {
        assertThrows(NullPointerException.class, () -> ackManager.createAck(null));
    }

    @Test
    void queryAcks_returns_false_for_pending_acks() {
        final long ackId = ackManager.createAck(CHANNEL);
        final Map<String, Boolean> result = ackManager.queryAcks(CHANNEL, Arrays.asList(ackId));

        assertThat(result.get(String.valueOf(ackId)), is(false));
    }

    @Test
    void queryAcks_returns_true_for_confirmed_acks() {
        final long ackId = ackManager.createAck(CHANNEL);
        ackManager.confirmAck(CHANNEL, ackId);
        final Map<String, Boolean> result = ackManager.queryAcks(CHANNEL, Arrays.asList(ackId));

        assertThat(result.get(String.valueOf(ackId)), is(true));
    }

    @Test
    void queryAcks_returns_false_for_unknown_ack_id() {
        ackManager.createAck(CHANNEL);
        final Map<String, Boolean> result = ackManager.queryAcks(CHANNEL, Arrays.asList(999L));

        assertThat(result.get("999"), is(false));
    }

    @Test
    void queryAcks_returns_false_for_unknown_channel() {
        final Map<String, Boolean> result = ackManager.queryAcks("unknown-channel", Arrays.asList(0L));

        assertThat(result.get("0"), is(false));
    }

    @Test
    void queryAcks_increments_counter() {
        ackManager.queryAcks(CHANNEL, Arrays.asList(0L));
        verify(ackRequestsCounter).increment();
    }

    @Test
    void confirmAck_increments_confirmed_counter() {
        final long ackId = ackManager.createAck(CHANNEL);
        ackManager.confirmAck(CHANNEL, ackId);
        verify(ackConfirmedCounter).increment();
    }

    @Test
    void confirmAck_twice_only_counts_once() {
        final long ackId = ackManager.createAck(CHANNEL);
        ackManager.confirmAck(CHANNEL, ackId);
        ackManager.confirmAck(CHANNEL, ackId);
        verify(ackConfirmedCounter, times(1)).increment();
    }

    @Test
    void confirmAck_for_unknown_channel_does_not_throw() {
        ackManager.confirmAck("unknown-channel", 0L);
        verify(ackConfirmedCounter, never()).increment();
    }

    @Test
    void confirmAck_for_unknown_ackId_does_not_throw() {
        ackManager.createAck(CHANNEL);
        ackManager.confirmAck(CHANNEL, 999L);
        verify(ackConfirmedCounter, never()).increment();
    }

    @Test
    void cleanupExpiredAcks_removes_expired_entries() {
        final HecAckManager shortExpiryManager = new HecAckManager(Duration.ofMillis(1), pluginMetrics);
        final long ackId = shortExpiryManager.createAck(CHANNEL);

        Awaitility.await()
                .atMost(Duration.ofSeconds(1))
                .pollDelay(Duration.ofMillis(10))
                .untilAsserted(() -> {
                    shortExpiryManager.cleanupExpiredAcks();
                    final Map<String, Boolean> result = shortExpiryManager.queryAcks(CHANNEL, Arrays.asList(ackId));
                    assertThat(result.get(String.valueOf(ackId)), is(false));
                });
        shortExpiryManager.shutdown();
    }

    @Test
    void cleanupExpiredAcks_does_not_count_confirmed_ack_as_expired() {
        final HecAckManager shortExpiryManager = new HecAckManager(Duration.ofMillis(1), pluginMetrics);
        final long ackId = shortExpiryManager.createAck(CHANNEL);
        shortExpiryManager.confirmAck(CHANNEL, ackId);

        Awaitility.await()
                .atMost(Duration.ofSeconds(1))
                .pollDelay(Duration.ofMillis(10))
                .untilAsserted(() -> {
                    shortExpiryManager.cleanupExpiredAcks();
                    final Map<String, Boolean> result = shortExpiryManager.queryAcks(CHANNEL, Arrays.asList(ackId));
                    assertThat(result.get(String.valueOf(ackId)), is(false));
                });
        verify(ackExpiredCounter, never()).increment();
        shortExpiryManager.shutdown();
    }

    @Test
    void shutdown_leaves_manager_queryable() {
        final long ackId = ackManager.createAck(CHANNEL);
        ackManager.shutdown();
        final Map<String, Boolean> result = ackManager.queryAcks(CHANNEL, Arrays.asList(ackId));
        assertThat(result.get(String.valueOf(ackId)), is(false));
    }

    @Test
    void removeAck_removes_unconfirmed_entry() {
        final long ackId = ackManager.createAck(CHANNEL);
        ackManager.removeAck(CHANNEL, ackId);
        ackManager.confirmAck(CHANNEL, ackId);
        verify(ackConfirmedCounter, never()).increment();
    }

    @Test
    void removeAck_for_confirmed_entry_does_not_double_count() {
        final long ackId = ackManager.createAck(CHANNEL);
        ackManager.confirmAck(CHANNEL, ackId);
        ackManager.removeAck(CHANNEL, ackId);
        final Map<String, Boolean> result = ackManager.queryAcks(CHANNEL, Arrays.asList(ackId));
        assertThat(result.get(String.valueOf(ackId)), is(false));
    }

    @Test
    void removeAck_for_unknown_channel_does_not_throw() {
        final long ackId = ackManager.createAck(CHANNEL);
        ackManager.removeAck("unknown-channel", 0L);
        final Map<String, Boolean> result = ackManager.queryAcks(CHANNEL, Arrays.asList(ackId));
        assertThat(result.get(String.valueOf(ackId)), is(false));
    }

    @Test
    void cleanupExpiredAcks_retains_channel_with_recent_entry() {
        final long recent = ackManager.createAck(CHANNEL);
        ackManager.cleanupExpiredAcks();
        ackManager.confirmAck(CHANNEL, recent);
        final Map<String, Boolean> result = ackManager.queryAcks(CHANNEL, Arrays.asList(recent));
        assertThat(result.get(String.valueOf(recent)), is(true));
    }

    @Test
    void cleanupExpiredAcks_with_huge_expiry_does_not_throw_and_retains_entries() {
        final HecAckManager hugeManager = new HecAckManager(Duration.ofSeconds(Long.MAX_VALUE), pluginMetrics);
        final long ackId = hugeManager.createAck(CHANNEL);
        hugeManager.cleanupExpiredAcks();
        hugeManager.confirmAck(CHANNEL, ackId);
        final Map<String, Boolean> result = hugeManager.queryAcks(CHANNEL, Arrays.asList(ackId));
        assertThat(result.get(String.valueOf(ackId)), is(true));
        hugeManager.shutdown();
    }
}
