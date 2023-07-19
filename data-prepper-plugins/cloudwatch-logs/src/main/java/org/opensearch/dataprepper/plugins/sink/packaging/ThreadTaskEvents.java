package org.opensearch.dataprepper.plugins.sink.packaging;

import org.opensearch.dataprepper.model.event.EventHandle;

import java.util.Collection;

/**
 * Simple data class for packaging event messages and their handles into a queue.
 */
public class ThreadTaskEvents {
    Collection<byte[]> eventMessages;
    Collection<EventHandle> eventHandles;
    public ThreadTaskEvents(Collection<byte[]> eventMessages, Collection<EventHandle> eventHandles) {
        this.eventMessages = eventMessages;
        this.eventHandles = eventHandles;
    }

    public Collection<byte[]> getEventMessages() {
        return eventMessages;
    }

    public Collection<EventHandle> getEventHandles() {
        return eventHandles;
    }
}
