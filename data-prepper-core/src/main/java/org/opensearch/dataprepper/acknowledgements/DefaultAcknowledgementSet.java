/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.acknowledgements;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class DefaultAcknowledgementSet implements AcknowledgementSet {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultAcknowledgementSet.class);
    private final Consumer<Boolean> callback;
    private final Instant expiryTime;
    private final ExecutorService executor;
    // This lock protects all the non-final members
    private final ReentrantLock lock;
    private boolean result;
    private final Map<EventHandle, AtomicInteger> pendingAcknowledgments;
    private Future<?> callbackFuture;
    private final DefaultAcknowledgementSetMetrics metrics;
    private boolean completed;

    public DefaultAcknowledgementSet(final ExecutorService executor, final Consumer<Boolean> callback, final Duration expiryTime, final DefaultAcknowledgementSetMetrics metrics) {
        this.callback = callback;
        this.result = true;
        this.executor = executor;
        this.expiryTime = Instant.now().plusMillis(expiryTime.toMillis());
        this.callbackFuture = null;
        this.metrics = metrics;
        this.completed = false;
        pendingAcknowledgments = new HashMap<>();
        lock = new ReentrantLock(true);
    }

    @Override
    public void add(Event event) {
        lock.lock();
        try {
            if (event instanceof JacksonEvent) {
                EventHandle eventHandle = event.getEventHandle();
                eventHandle.setAcknowledgementSet(this);
                pendingAcknowledgments.put(eventHandle, new AtomicInteger(1));
            }
        } finally {
            lock.unlock();
        }
    }

    public void acquire(final EventHandle eventHandle) {
        lock.lock();
        try {
            if (!pendingAcknowledgments.containsKey(eventHandle)) {
                LOG.warn("Unexpected event handle acquire");
                metrics.increment(DefaultAcknowledgementSetMetrics.INVALID_ACQUIRES_METRIC_NAME);
                return;
            }
            pendingAcknowledgments.get(eventHandle).incrementAndGet();
        } finally {
            lock.unlock();
        }
    }

    public boolean isDone() {
        lock.lock();
        try {
            if (callbackFuture != null && callbackFuture.isDone()) {
                metrics.increment(DefaultAcknowledgementSetMetrics.COMPLETED_METRIC_NAME);
                return true;
            }
            if (Instant.now().isAfter(expiryTime)) {
                if (callbackFuture != null) {
                    callbackFuture.cancel(true);
                    callbackFuture = null;
                    LOG.warn("AcknowledgementSet expired");
                }
                metrics.increment(DefaultAcknowledgementSetMetrics.EXPIRED_METRIC_NAME);
                return true;
            }
        } finally {
            lock.unlock();
        }
        return false;
    }

    public Instant getExpiryTime() {
        return expiryTime;
    }

    @Override
    public void complete() {
        lock.lock();
        try {
            completed = true;
            if (pendingAcknowledgments.size() == 0) {
                callbackFuture = executor.submit(() -> callback.accept(this.result));
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean release(final EventHandle eventHandle, final boolean result) {
        lock.lock();
        // Result indicates negative or positive acknowledgement. Even if one of the
        // events in the set report negative acknowledgement, then the end result
        // is negative acknowledgement
        this.result = this.result && result;
        try {
            if (!pendingAcknowledgments.containsKey(eventHandle) ||
                pendingAcknowledgments.get(eventHandle).get() == 0) {
                LOG.warn("Unexpected event handle release");
                metrics.increment(DefaultAcknowledgementSetMetrics.INVALID_RELEASES_METRIC_NAME);
                return false;
            }
            if (pendingAcknowledgments.get(eventHandle).decrementAndGet() == 0) {
                pendingAcknowledgments.remove(eventHandle);
                if (completed && pendingAcknowledgments.size() == 0) {
                    callbackFuture = executor.submit(() -> callback.accept(this.result));
                    return true;
                } else if (pendingAcknowledgments.size() == 0) {
                    LOG.warn("Acknowledgement set is not completed. Delaying callback until it is completed");
                }
            }
        } finally {
            lock.unlock();
        }
        return false;
    }
}
