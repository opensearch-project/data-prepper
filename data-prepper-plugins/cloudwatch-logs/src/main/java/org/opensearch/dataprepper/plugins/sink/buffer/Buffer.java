package org.opensearch.dataprepper.plugins.sink.buffer;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

/**
 * Buffer that handles the temporary storage of
 * events. It isolates the implementation of system storage.
 */
public interface Buffer {
    /**
     * Size of buffer in bytes.
     * @return
     */
    int getEventCount();

    int getBufferSize();

    void writeEvent(Record<Event> event);

    Record<Event> getEvent();
}