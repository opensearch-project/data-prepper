/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.buffer;

import org.opensearch.dataprepper.model.event.EventHandle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class InMemoryBuffer implements Buffer {
    private List<byte[]> eventsBuffered;
    private int bufferSize = 0;
    private List<EventHandle> eventHandles;

    InMemoryBuffer() {
        eventsBuffered = new ArrayList<>();
        eventHandles = new ArrayList<>();
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
    public List<EventHandle> getEventHandles() {
        return Collections.unmodifiableList(eventHandles);
    }

    @Override
    public void writeEvent(final EventHandle eventHandle, final byte[] event) {
        eventHandles.add(eventHandle);
        eventsBuffered.add(event);
        bufferSize += event.length;
    }

    @Override
    public byte[] popEvent() {
        if (eventsBuffered.isEmpty()) {
            return new byte[0];
        }
        bufferSize -= eventsBuffered.get(0).length;
        return eventsBuffered.remove(0);
    }

    @Override
    public List<byte[]> getBufferedData() {
        return Collections.unmodifiableList(eventsBuffered);
    }

    @Override
    public void clearBuffer() {
        bufferSize = 0;
        eventsBuffered.clear();
    }

    @Override
    public void resetBuffer() {
        bufferSize = 0;
        eventsBuffered = new ArrayList<>();
        eventHandles = new ArrayList<>();
    }
}
