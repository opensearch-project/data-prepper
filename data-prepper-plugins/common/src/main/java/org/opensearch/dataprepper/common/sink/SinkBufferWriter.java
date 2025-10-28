/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.common.sink;

public interface SinkBufferWriter {
    public boolean writeToBuffer(SinkBufferEntry sinkBufferEntry);
    public SinkFlushableBuffer getBuffer(final SinkFlushContext  sinkFlushContext);
}
