/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.common.sink;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import com.linecorp.armeria.client.retry.Backoff;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.locks.ReentrantLock;

public abstract class DefaultSinkOutputStrategy implements SinkOutputStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultSinkOutputStrategy.class);
    private final ReentrantLock reentrantLock;

    public DefaultSinkOutputStrategy() {
        reentrantLock = new ReentrantLock();
    }

    public void execute(Collection<Record<Event>> records) {
        // If records are empty check if buffer needs to be flushed based on flush interval
        if (records.isEmpty()) {
            lock();
            try {
                if (exceedsFlushTimeInterval()) {
                    flushBuffer();
                }
            } finally {
                unlock();
            }
            pushDLQList();
            return;
        }
        lock();
        try {
            for (Record<Event> record : records) {
                final Event event = record.getData();
                try {
                    long estimatedSize = getEstimatedSize(event);
                    if (exceedsMaxEventSizeThreshold(estimatedSize)) {
                        throw new RuntimeException("Event size exceeds max allowed event size");
                    }
                    if (willExceedMaxRequestSizeBytes(event, estimatedSize)) {
                        flushBuffer();
                    }
                    boolean reachedMaxEventsLimit = addToBuffer(event, estimatedSize);
                    if (reachedMaxEventsLimit) {
                        flushBuffer();
                    } 
                } catch (Exception ex) {
                    addEventToDLQList(event, ex);
                }
            }
            pushDLQList();
        } finally {
            pushDLQList();
            unlock();
        }
    }

    public abstract void flushBuffer();
    public abstract void pushDLQList();
    public abstract void addEventToDLQList(final Event event, Throwable ex);
    public abstract boolean addToBuffer(final Event event, final long estimatedSize) throws Exception;
    public abstract boolean exceedsFlushTimeInterval();
    public abstract boolean willExceedMaxRequestSizeBytes(final Event event, final long estimatedSize) throws Exception;
    public abstract boolean exceedsMaxEventSizeThreshold(final long estimatedSize);
    public abstract long getEstimatedSize(final Event event) throws Exception;
    public abstract void recordLatency(double latency);

    @Override
    public void lock() {
        reentrantLock.lock();
    }

    @Override
    public void unlock() {
        reentrantLock.unlock();
    }

}
