/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.buffer;

import java.util.ArrayList;

public class InMemoryBuffer implements Buffer {
    private final ArrayList<byte[]> eventsBuffered;
    private int bufferSize = 0;

    InMemoryBuffer() {
        eventsBuffered = new ArrayList<>();
    }

    @Override
    public int getEventCount() {
        return eventsBuffered.size();
    }

    @Override
    public int getBufferSize() {
        return bufferSize;
    }

    @Override
    public void writeEvent(final byte[] event) {
        eventsBuffered.add(event);
        bufferSize += event.length;
    }

    @Override
    public byte[] popEvent() {
        bufferSize -= eventsBuffered.get(0).length;
        return eventsBuffered.remove(0);
    }

    @Override
    public ArrayList<byte[]> getBufferedData() {
        return eventsBuffered;
    }

    @Override
    public void clearBuffer() {
        bufferSize = 0;
        eventsBuffered.clear();
    }
}