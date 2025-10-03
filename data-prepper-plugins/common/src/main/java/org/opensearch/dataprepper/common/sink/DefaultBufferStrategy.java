/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.common.sink;

import org.opensearch.dataprepper.model.event.Event;

public abstract class DefaultBufferStrategy implements BufferStrategy {
    @Override
    public abstract boolean addToBuffer(final Event event, final long estimatedSize) throws Exception;

    @Override
    public abstract void flushBuffer();

    @Override
    public abstract boolean exceedsFlushTimeInterval();

    @Override
    public abstract boolean willExceedMaxRequestSizeBytes(final Event event, final long estimatedSize) throws Exception;

    @Override
    public abstract boolean exceedsMaxEventSizeThreshold(final long estimatedSize);

    @Override
    public abstract long getEstimatedSize(final Event event) throws Exception;

    @Override
    public abstract void recordLatency(double latency);
}