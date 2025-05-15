/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstract base class for test processors that track event processing.
 * This class provides common functionality for tracking all the events that are processed
 */
public abstract class BaseEventsTrackingProcessor implements Processor<Record<Event>, Record<Event>> {
    private final String countPropertyName;
    private final String threadPropertyName;
    private final Map<String, AtomicInteger> eventsMap;
    private final String processorName;

    /**
     * Constructor for the base events tracking processor.
     *
     * @param processorName Name of the processor
     * @param eventsMap Map for tracking processed events
     */
    protected BaseEventsTrackingProcessor(String processorName, Map<String, AtomicInteger> eventsMap) {
        this.countPropertyName = processorName + "_processed_count";
        this.threadPropertyName = processorName + "_processed_by_thread";
        this.processorName = processorName;
        this.eventsMap = eventsMap;
    }

    /**
     * Gets the map of processed events.
     * @return Map of event IDs to processing counts
     */
    public Map<String, AtomicInteger> getEventsMap() {
        return eventsMap;
    }

    /**
     * Gets the name of processor.
     * @return The processor name
     */
    public String getName() {
        return processorName;
    }

    /**
     * Resets the processor's state by clearing the events map.
     */
    public void reset() {
        eventsMap.clear();
    }

    /**
     * Processes a collection of event records.
     * For each event:
     * 1. Stores the event which was provided for processing in the events map
     * 2. Records the processing count in the event metadata
     * 3. Records which thread processed the event in the event metadata
     *
     * @param records Collection of event records to process
     * @return records with added metadata
     */
    @Override
    public Collection<Record<Event>> execute(final Collection<Record<Event>> records) {
        final String threadName = Thread.currentThread().getName();

        for (Record<Event> record : records) {
            Event event = record.getData();
            String eventId = event.get("id", String.class);

            if (eventId != null) {
                AtomicInteger counter = eventsMap.computeIfAbsent(eventId, id -> new AtomicInteger(0));
                int count = counter.incrementAndGet();

                event.put(countPropertyName, count);
                event.put(threadPropertyName, threadName);
            }
        }
        return records;
    }

    @Override
    public void prepareForShutdown() {
    }

    @Override
    public boolean isReadyForShutdown() {
        return true;
    }

    @Override
    public void shutdown() {
    }
}
