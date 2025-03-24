/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.buffer;

import org.opensearch.dataprepper.model.event.EventHandle;
import java.util.List;

/**
 * Buffer that handles the temporary storage of
 * events. It isolates the implementation of system storage.
 * 1. Reads in a String.
 * 2. Transforms to Byte type.
 * 3. Returns a Byte type.
 */

/*
    TODO:
     Need to add PriorityQueue for extracting timestamp, this will need the timestamp and the actual string message itself.
     Can refactor the buffer to contain
 */

public interface Buffer {
    /**
     * Size of buffer in events.
     * @return int
     */
    int getEventCount();

    /**
     * Size of buffer in bytes.
     * @return int
     */
    int getBufferSize();

    void writeEvent(EventHandle eventHandle, byte[] event);

    List<EventHandle> getEventHandles();

    byte[] popEvent();

    List<byte[]> getBufferedData();

    void clearBuffer();

    void resetBuffer();
}
