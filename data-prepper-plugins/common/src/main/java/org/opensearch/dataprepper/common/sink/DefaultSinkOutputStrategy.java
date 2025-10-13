/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.common.sink;


import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public abstract class DefaultSinkOutputStrategy implements SinkOutputStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultSinkOutputStrategy.class);
    private final LockStrategy lockStrategy;
    private final BufferStrategy bufferStrategy;
    private final SinkMetrics sinkMetrics;

    public DefaultSinkOutputStrategy(final LockStrategy lockStrategy, final BufferStrategy bufferStrategy, final SinkMetrics sinkMetrics) {
        this.lockStrategy = lockStrategy;
        this.bufferStrategy = bufferStrategy;
        this.sinkMetrics = sinkMetrics;
    }

    SinkMetrics getSinkMetrics() {
        return sinkMetrics;
    }

    private void flushBuffer() {
        long startTime = System.nanoTime();
        boolean flushSucceeded = bufferStrategy.flushBuffer(sinkMetrics);
        if (flushSucceeded) {
            sinkMetrics.recordRequestLatency((double)(System.nanoTime() - startTime));
        }
    }

    public void execute(Collection<Record<Event>> records) {
        lockStrategy.lock();
        try {
            // If records are empty, check if batch buffer needs to be flushed
            // based on flush interval
            if (records.isEmpty()) {
                if (bufferStrategy.exceedsFlushTimeInterval()) {
                    flushBuffer();
                }
            } else {
                for (Record<Event> record : records) {
                    final Event event = record.getData();
                    try {
                        long estimatedSize = bufferStrategy.getEstimatedSize(event);
                        // Check if individual event exceeds sink's max event size
                        if (bufferStrategy.exceedsMaxEventSizeThreshold(estimatedSize)) {
                            throw new RuntimeException("Event size exceeds max allowed event size");
                        }
                        // Check if adding this event to the batch buffer, would exceed the batch
                        // buffer's max bytes threshold, if yes, flush the batch buffer
                        if (bufferStrategy.willExceedMaxRequestSizeBytes(event, estimatedSize)) {
                            flushBuffer();
                        }
                        long numEvents = bufferStrategy.addToBuffer(event, estimatedSize);
                        // Check if after adding the event, max events in a batch threshold exceeded
                        // If yes, flush the batch buffer
                        if (bufferStrategy.isMaxEventsLimitReached(numEvents)) {
                            flushBuffer();
                        } 
                    } catch (Exception ex) {
                        LOG.warn(NOISY, "Failed process the event ", ex);
                        addEventToDLQList(event, ex);
                    }
                }
            }
        } finally {
            flushDLQList();
            lockStrategy.unlock();
        }
    }

    public abstract void flushDLQList();
    public abstract void addEventToDLQList(final Event event, Throwable ex);

}
