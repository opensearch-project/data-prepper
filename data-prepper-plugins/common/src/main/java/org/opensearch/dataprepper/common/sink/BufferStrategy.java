/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.common.sink;

import org.opensearch.dataprepper.model.event.Event;

public interface BufferStrategy {
    boolean addToBuffer(final Event event, final long estimatedSize) throws Exception;
    void flushBuffer();
    boolean exceedsFlushTimeInterval();
    boolean willExceedMaxRequestSizeBytes(final Event event, final long estimatedSize) throws Exception;
    boolean exceedsMaxEventSizeThreshold(final long estimatedSize);
    long getEstimatedSize(final Event event) throws Exception;
    void recordLatency(double latency);
}