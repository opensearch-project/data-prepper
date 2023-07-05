/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.buffer;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.util.ArrayList;
import java.util.Optional;

//TODO: Might not be thread safe to use static variables in a buffer that can be shared.
public class InMemoryBuffer implements Buffer {
    private ArrayList<byte[]> eventsBuffered;
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
    public byte[] getEvent() {
        bufferSize -= eventsBuffered.get(0).length;
        return eventsBuffered.remove(0);
    }
}