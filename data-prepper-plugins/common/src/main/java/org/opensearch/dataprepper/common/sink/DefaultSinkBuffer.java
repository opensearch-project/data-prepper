 /*
  * Copyright OpenSearch Contributors
  * SPDX-License-Identifier: Apache-2.0
  *
  * The OpenSearch Contributors require contributions made to
  * this file be licensed under the Apache-2.0 license or a
  * compatible open source license.
  *
  */
package org.opensearch.dataprepper.common.sink;

import java.time.Instant;

public class DefaultSinkBuffer implements SinkBuffer {
    protected final SinkBufferWriter sinkBufferWriter;
    protected final long maxEvents;
    protected final long maxRequestSize;
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
        long curTime = System.currentTimeMillis();
        return (curTime - lastFlushedTimeMs >= flushIntervalMs);
    }

    @Override
    public boolean willExceedMaxRequestSizeBytes(final SinkBufferEntry sinkBufferEntry) {
        return (currentRequestSize + sinkBufferEntry.getEstimatedSize() >= maxRequestSize);
    }

    @Override
    public SinkFlushableBuffer getFlushableBuffer(final SinkFlushContext sinkFlushContext) {
        numEvents = 0;
        currentRequestSize = 0L;
        lastFlushedTimeMs = System.currentTimeMillis();
        return sinkBufferWriter.getBuffer(sinkFlushContext);
    }

}

