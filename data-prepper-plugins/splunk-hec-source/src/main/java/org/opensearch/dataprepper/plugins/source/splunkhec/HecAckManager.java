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
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class HecAckManager {

    static final String ACK_REQUESTS_TOTAL = "ackRequestsTotal";
    static final String ACK_CONFIRMED_TOTAL = "ackConfirmedTotal";
    static final String ACK_PENDING = "ackPending";
    static final String ACK_EXPIRED_TOTAL = "ackExpiredTotal";

    private static final long CLEANUP_INTERVAL_SECONDS = 60;

    private static final Logger LOG = LoggerFactory.getLogger(HecAckManager.class);

    private final ConcurrentHashMap<String, ChannelState> channelStates;
    private final Duration ackExpiry;
    private final ScheduledExecutorService cleanupExecutor;
    private final Counter ackRequestsCounter;
    private final Counter ackConfirmedCounter;
    private final AtomicLong pendingAcks;
    private final Counter ackExpiredCounter;

    public HecAckManager(final Duration ackExpiry, final PluginMetrics pluginMetrics) {
        Objects.requireNonNull(ackExpiry, "ackExpiry must not be null");
        Objects.requireNonNull(pluginMetrics, "pluginMetrics must not be null");
        this.ackExpiry = ackExpiry;
        this.channelStates = new ConcurrentHashMap<>();
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            final Thread thread = new Thread(r);
            thread.setDaemon(true);
            thread.setName("splunk-hec-ack-cleanup");
            return thread;
        });
        this.cleanupExecutor.scheduleWithFixedDelay(this::cleanupExpiredAcks,
                CLEANUP_INTERVAL_SECONDS, CLEANUP_INTERVAL_SECONDS, TimeUnit.SECONDS);

        this.ackRequestsCounter = pluginMetrics.counter(ACK_REQUESTS_TOTAL);
        this.ackConfirmedCounter = pluginMetrics.counter(ACK_CONFIRMED_TOTAL);
        this.pendingAcks = new AtomicLong(0);
        pluginMetrics.gauge(ACK_PENDING, pendingAcks, AtomicLong::doubleValue);
        this.ackExpiredCounter = pluginMetrics.counter(ACK_EXPIRED_TOTAL);
    }

    public long createAck(final String channel) {
        Objects.requireNonNull(channel, "channel must not be null");
        final long[] ackIdHolder = new long[1];
        channelStates.compute(channel, (k, existing) -> {
            final ChannelState state = existing == null ? new ChannelState() : existing;
            final long ackId = state.nextAckId.getAndIncrement();
            state.ackEntries.put(ackId, new AckEntry(Instant.now()));
            ackIdHolder[0] = ackId;
            return state;
        });
        pendingAcks.incrementAndGet();
        return ackIdHolder[0];
    }

    public void confirmAck(final String channel, final long ackId) {
        final ChannelState state = channelStates.get(channel);
        if (state == null) {
            LOG.debug("Received a confirmation for an unknown or expired channel {}; dropping ack id {}.", channel, ackId);
            return;
        }
        final AckEntry entry = state.ackEntries.get(ackId);
        if (entry == null) {
            LOG.debug("Received a confirmation for an unknown or expired ack id {} on channel {}; dropping it.", ackId, channel);
            return;
        }
        if (entry.confirmed.compareAndSet(false, true)) {
            ackConfirmedCounter.increment();
        }
        if (entry.accounted.compareAndSet(false, true)) {
            pendingAcks.decrementAndGet();
        }
    }

    public void removeAck(final String channel, final long ackId) {
        final ChannelState state = channelStates.get(channel);
        if (state == null) {
            return;
        }
        final AckEntry entry = state.ackEntries.remove(ackId);
        if (entry != null && entry.accounted.compareAndSet(false, true)) {
            pendingAcks.decrementAndGet();
        }
    }

    public Map<String, Boolean> queryAcks(final String channel, final Iterable<Long> ackIds) {
        ackRequestsCounter.increment();
        final Map<String, Boolean> results = new HashMap<>();
        final ChannelState state = channelStates.get(channel);
        for (final Long ackId : ackIds) {
            if (state == null) {
                results.put(String.valueOf(ackId), false);
                continue;
            }
            final AckEntry entry = state.ackEntries.get(ackId);
            if (entry == null) {
                results.put(String.valueOf(ackId), false);
            } else {
                results.put(String.valueOf(ackId), entry.confirmed.get());
            }
        }
        return results;
    }

    public void shutdown() {
        cleanupExecutor.shutdown();
    }

    void cleanupExpiredAcks() {
        final Instant cutoff;
        try {
            cutoff = Instant.now().minus(ackExpiry);
        } catch (final ArithmeticException | DateTimeException e) {
            LOG.warn("Skipping ack cleanup for ack expiry {}: {}", ackExpiry, e.getMessage());
            return;
        }
        for (final String channel : channelStates.keySet()) {
            channelStates.compute(channel, (k, state) -> {
                if (state == null) {
                    return null;
                }
                final Iterator<Map.Entry<Long, AckEntry>> it = state.ackEntries.entrySet().iterator();
                while (it.hasNext()) {
                    final Map.Entry<Long, AckEntry> entry = it.next();
                    if (entry.getValue().createdAt.isBefore(cutoff)) {
                        if (entry.getValue().accounted.compareAndSet(false, true)) {
                            pendingAcks.decrementAndGet();
                            ackExpiredCounter.increment();
                        }
                        it.remove();
                    }
                }
                return state.ackEntries.isEmpty() ? null : state;
            });
        }
    }

    private static class ChannelState {
        private final AtomicLong nextAckId = new AtomicLong(0);
        private final ConcurrentHashMap<Long, AckEntry> ackEntries = new ConcurrentHashMap<>();
    }

    private static class AckEntry {
        private final Instant createdAt;
        private final AtomicBoolean confirmed = new AtomicBoolean(false);
        private final AtomicBoolean accounted = new AtomicBoolean(false);

        AckEntry(final Instant createdAt) {
            this.createdAt = createdAt;
        }
    }
}
