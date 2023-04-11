/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.event.EventBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Provides a mechanism to write records to an in_memory source. This allows the pipeline to execute
 * independently of the test code, but the test code can write to a source.
 */
public class InMemorySourceAccessor {
    private final Map<String, List<Record<Event>>> recordsMap = new HashMap<>();
    private EventFactory eventFactory;
    private AtomicBoolean ackReceived;
    private final Lock lock;

    public InMemorySourceAccessor() {
        this.lock = new ReentrantLock();
    }

    public void setEventFactory(final EventFactory factory) {
        eventFactory = factory;
    }

    public void setAckReceived(boolean newValue) {
        if (ackReceived == null) {
            ackReceived = new AtomicBoolean(newValue);
        } else {
            ackReceived.set(newValue);
        }
    }

    public Boolean getAckReceived() {
        if (ackReceived == null) {
            return null;
        }
        return ackReceived.get();
    }

    public void submit(final String testingKey, int numRecords) {
        if (eventFactory == null) {
            return;
        }
        List<Record<Event>> records = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            Map<String, Object> eventMap = Map.of("message", UUID.randomUUID().toString());
            EventBuilder eventBuilder = (EventBuilder) eventFactory.eventBuilder(EventBuilder.class).withData(eventMap);
            JacksonEvent event = (JacksonEvent) eventBuilder.build();
            records.add(new Record<Event>(event));
        }
        submit(testingKey, records);
    }

    public void submitWithStatus(final String testingKey, int numRecords) {
        if (eventFactory == null) {
            return;
        }
        List<Record<Event>> records = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            final int max = 600;
            final int min = 100;
            int status = (int)(Math.random() * (max - min + 1) + min);
            Map<String, Object> eventMap = Map.of("message", UUID.randomUUID().toString(), "status", status);
            EventBuilder eventBuilder = (EventBuilder) eventFactory.eventBuilder(EventBuilder.class).withData(eventMap);
            JacksonEvent event = (JacksonEvent) eventBuilder.build();
            records.add(new Record<Event>(event));
        }
        submit(testingKey, records);
    }

    /**
     * Submits records to the in_memory source. These will be available to the source
     * for reading.
     *
     * @param testingKey The key for the in_memory source
     * @param newRecords New records to add.
     */
    public void submit(final String testingKey, final List<Record<Event>> newRecords) {
        lock.lock();
        try {
            recordsMap.computeIfAbsent(testingKey, i -> new ArrayList<>())
                    .addAll(newRecords);
        } finally {
            lock.unlock();
        }
    }

    List<Record<Event>> read(final String testingKey) {
        final List<Record<Event>> records;
        lock.lock();
        try {
            records = recordsMap.getOrDefault(testingKey, Collections.emptyList());

            recordsMap.remove(testingKey);
        } finally {
            lock.unlock();
        }

        return records;
    }
}
