/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.common.sink;

import java.time.Instant;

public class DefaultSinkBuffer implements SinkBuffer {
    private final SinkBufferWriter sinkBufferWriter;
    private final long maxEvents;
    private final long maxRequestSize;
    private final long flushIntervalMs;
    private long lastFlushedTimeMs;
    private long numEvents;
    private long currentRequestSize;

    public DefaultSinkBuffer(final long maxEvents, 
        final long maxRequestSize, 
        final long flushIntervalMs, final SinkBufferWriter sinkBufferWriter) {
            this.maxEvents = maxEvents;
            this.maxRequestSize = maxRequestSize;
            this.flushIntervalMs = flushIntervalMs;
            this.sinkBufferWriter = sinkBufferWriter;
            lastFlushedTimeMs = Instant.now().toEpochMilli();
            numEvents = 0L;
            currentRequestSize = 0L;
    }
    
    @Override
    public boolean addToBuffer(final SinkBufferEntry sinkBufferEntry) throws Exception {
        if (sinkBufferWriter.writeToBuffer(sinkBufferEntry)) {
            currentRequestSize += sinkBufferEntry.getEstimatedSize();
            numEvents++;
            return true;
        }
        return false;
    }

    @Override
    public boolean isMaxEventsLimitReached() {
        return numEvents >= maxEvents;
    }

    @Override
    public boolean exceedsFlushTimeInterval() {
        long curTime = Instant.now().toEpochMilli();
        return (curTime - lastFlushedTimeMs >= flushIntervalMs);
    }

    @Override
    public boolean willExceedMaxRequestSizeBytes(final SinkBufferEntry sinkBufferEntry) {
        return (currentRequestSize + sinkBufferEntry.getEstimatedSize() >= maxRequestSize);
    }

    @Override
    public SinkFlushableBuffer getFlushableBuffer(final SinkFlushContext sinkFlushContext) {
        return sinkBufferWriter.getBuffer(sinkFlushContext);
    }

}

