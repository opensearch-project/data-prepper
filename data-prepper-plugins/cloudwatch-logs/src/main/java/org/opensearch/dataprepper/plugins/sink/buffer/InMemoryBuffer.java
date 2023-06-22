package org.opensearch.dataprepper.plugins.sink.buffer;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.util.ArrayList;
import java.util.Optional;

//TODO: Might not be thread safe to use static variables in a buffer that can be shared.
public class InMemoryBuffer implements Buffer {
    private ArrayList<Record<Event>> eventsBuffered;
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
    public void writeEvent(Record<Event> event) {
        eventsBuffered.add(event);
        bufferSize += event.getData().toJsonString().length();
    }

    @Override
    public Record<Event> getEvent() {
        bufferSize -= eventsBuffered.get(0).getData().toJsonString().length();
        return eventsBuffered.remove(0);
    }
}
