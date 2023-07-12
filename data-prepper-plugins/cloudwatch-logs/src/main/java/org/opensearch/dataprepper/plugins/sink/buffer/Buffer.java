/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.buffer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Buffer that handles the temporary storage of
 * events. It isolates the implementation of system storage.
 * 1. Reads in a String.
 * 2. Transforms to Byte type.
 * 3. Returns a Byte type.
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

    void writeEvent(byte[] event);

    byte[] popEvent();

    ArrayList<byte[]> getBufferedData();

    void clearBuffer();
}