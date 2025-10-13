/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.common.sink;

import org.opensearch.dataprepper.model.event.Event;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public abstract class DefaultBufferStrategy implements BufferStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultBufferStrategy.class);
    long lastFlushedTime;
    final long maxEventSize;
    final long maxRequestSize;
    final long flushIntervalMs;
    final long maxEvents;

    public DefaultBufferStrategy(final long maxEventSize, final long maxEvents, final long maxRequestSize, final long flushIntervalMs) {
        lastFlushedTime = Instant.now().toEpochMilli();
        this.maxEventSize = maxEventSize;
        this.maxEvents = maxEvents;
        this.maxRequestSize = maxRequestSize;
        this.flushIntervalMs = flushIntervalMs;
    }

    @Override
    public boolean flushBuffer(final SinkMetrics sinkMetrics) {
        try {
            Object failedStatus = doFlush(null);
            lastFlushedTime = Instant.now().toEpochMilli();
            return failedStatus == null;
        } catch (Exception e) {
            LOG.error(NOISY, "Failed to flush.", e);
        }
        return false;
    }

    @Override
    public boolean isMaxEventsLimitReached(long numEvents) {
        return numEvents >= maxEvents;
    }

    @Override
    public boolean exceedsFlushTimeInterval() {
        long curTime = Instant.now().toEpochMilli();
        return (curTime - lastFlushedTime >= flushIntervalMs);
    }

    @Override
    public boolean willExceedMaxRequestSizeBytes(final Event event, final long estimatedSize) {
        return (getCurrentRequestSize() + estimatedSize >= maxRequestSize);
    }

    @Override
    public boolean exceedsMaxEventSizeThreshold(final long estimatedSize) {
        return estimatedSize >= maxEventSize;
    }

    public abstract Object doFlush(Object failedStatus) throws Exception;

    public abstract long getCurrentRequestSize();
}
