/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.common.sink;

import org.opensearch.dataprepper.model.event.Event;

import java.time.Instant;

public abstract class DefaultBufferStrategy implements BufferStrategy {
    long lastFlushedTime;
    final long maxEventSize;
    final long maxRequestSize;
    final long flushIntervalMs;

    public DefaultBufferStrategy(final long maxEventSize, final long maxRequestSize, final long flushIntervalMs) {
        lastFlushedTime = Instant.now().toEpochMilli();
        this.maxEventSize = maxEventSize;
        this.maxRequestSize = maxRequestSize;
        this.flushIntervalMs = flushIntervalMs;
    }

    @Override
    public abstract boolean addToBuffer(final Event event, final long estimatedSize) throws Exception;

    @Override
    public boolean flushBuffer(final SinkMetrics sinkMetrics) {
        Object failedStatus = doFlushOnce(null);
        lastFlushedTime = Instant.now().toEpochMilli();
        return failedStatus == null;
    }

    @Override
    public boolean exceedsFlushTimeInterval() {
        long curTime = Instant.now().toEpochMilli();
        return (curTime - lastFlushedTime >= flushIntervalMs);
    }

    @Override
    public boolean willExceedMaxRequestSizeBytes(final Event event, final long estimatedSize) throws Exception {
        return (getCurrentRequestSize() + estimatedSize >= maxRequestSize);
    }

    @Override
    public boolean exceedsMaxEventSizeThreshold(final long estimatedSize) {
        return estimatedSize >= maxEventSize;
    }

    public abstract Object doFlushOnce(Object failedStatus);

    public abstract long getCurrentRequestSize();
}
