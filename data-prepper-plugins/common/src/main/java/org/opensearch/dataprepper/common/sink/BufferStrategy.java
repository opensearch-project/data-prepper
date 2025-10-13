/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.common.sink;

import org.opensearch.dataprepper.model.event.Event;

public interface BufferStrategy {
    long addToBuffer(final Event event, final long estimatedSize) throws Exception;
    boolean flushBuffer(SinkMetrics sinkMetrics);
    boolean exceedsFlushTimeInterval();
    boolean willExceedMaxRequestSizeBytes(final Event event, final long estimatedSize);
    boolean isMaxEventsLimitReached(long numEvents);
    boolean exceedsMaxEventSizeThreshold(final long estimatedSize);
    long getEstimatedSize(final Event event) throws Exception;
}
