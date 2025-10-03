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
    private final LockStrategy lockStrategy;
    private final BufferStrategy bufferStrategy;

    public DefaultSinkOutputStrategy(LockStrategy lockStrategy, BufferStrategy bufferStrategy) {
        this.lockStrategy = lockStrategy;
        this.bufferStrategy = bufferStrategy;
    }

    public void execute(Collection<Record<Event>> records) {
        // If records are empty check if buffer needs to be flushed based on flush interval
        if (records.isEmpty()) {
            lockStrategy.lock();
            try {
                if (bufferStrategy.exceedsFlushTimeInterval()) {
                    bufferStrategy.flushBuffer();
                }
            } finally {
                lockStrategy.unlock();
            }
            pushDLQList();
            return;
        }
        lockStrategy.lock();
        try {
            for (Record<Event> record : records) {
                final Event event = record.getData();
                try {
                    long estimatedSize = bufferStrategy.getEstimatedSize(event);
                    if (bufferStrategy.exceedsMaxEventSizeThreshold(estimatedSize)) {
                        throw new RuntimeException("Event size exceeds max allowed event size");
                    }
                    if (bufferStrategy.willExceedMaxRequestSizeBytes(event, estimatedSize)) {
                        bufferStrategy.flushBuffer();
                    }
                    boolean reachedMaxEventsLimit = bufferStrategy.addToBuffer(event, estimatedSize);
                    if (reachedMaxEventsLimit) {
                        bufferStrategy.flushBuffer();
                    } 
                } catch (Exception ex) {
                    addEventToDLQList(event, ex);
                }
            }
            pushDLQList();
        } finally {
            pushDLQList();
            lockStrategy.unlock();
        }
    }

    public abstract void pushDLQList();
    public abstract void addEventToDLQList(final Event event, Throwable ex);

}
