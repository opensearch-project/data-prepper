/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.http.service;

import org.opensearch.dataprepper.common.sink.SinkBufferEntry;
import org.opensearch.dataprepper.common.sink.SinkBufferWriter;
import org.opensearch.dataprepper.common.sink.SinkFlushContext;
import org.opensearch.dataprepper.common.sink.SinkFlushableBuffer;
import org.opensearch.dataprepper.common.sink.SinkMetrics;

import java.util.ArrayList;
import java.util.List;

public class HttpSinkBufferWriter implements SinkBufferWriter {
    private final List<SinkBufferEntry> buffer;
    private final SinkMetrics sinkMetrics;
    private long currentSize;

    public HttpSinkBufferWriter(final SinkMetrics sinkMetrics) {
        this.buffer = new ArrayList<>();
        this.sinkMetrics = sinkMetrics;
        this.currentSize = 0;
    }

    @Override
    public boolean writeToBuffer(final SinkBufferEntry sinkBufferEntry) {
        buffer.add(sinkBufferEntry);
        currentSize += sinkBufferEntry.getEstimatedSize();
        return true;
    }

    @Override
    public SinkFlushableBuffer getBuffer(final SinkFlushContext sinkFlushContext) {
        if (buffer.isEmpty()) {
            return null;
        }
        final List<SinkBufferEntry> bufferCopy = new ArrayList<>(buffer);
        final long requestSize = currentSize;
        buffer.clear();
        currentSize = 0;
        sinkMetrics.recordRequestSize(requestSize);
        return new HttpSinkFlushableBuffer(bufferCopy, sinkMetrics, sinkFlushContext);
    }

    @Override
    public boolean isMaxEventsLimitReached(final long maxEvents) {
        return buffer.size() >= maxEvents;
    }

    @Override
    public boolean willExceedMaxRequestSizeBytes(final SinkBufferEntry sinkBufferEntry, final long maxRequestSize) {
        return currentSize + sinkBufferEntry.getEstimatedSize() > maxRequestSize;
    }
}
