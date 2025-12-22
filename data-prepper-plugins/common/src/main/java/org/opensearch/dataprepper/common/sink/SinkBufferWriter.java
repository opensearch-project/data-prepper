/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.common.sink;

public interface SinkBufferWriter {
    public boolean writeToBuffer(SinkBufferEntry sinkBufferEntry);
    public SinkFlushableBuffer getBuffer(final SinkFlushContext  sinkFlushContext);
    default boolean isMaxEventsLimitReached(final long maxEvents) {
        return false;
    }

    default boolean willExceedMaxRequestSizeBytes(final SinkBufferEntry sinkBufferEntry, final long maxRequestSize) {
        return false;
    }
}
