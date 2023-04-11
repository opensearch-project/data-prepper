/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Provides a mechanism for accessing the records output by {@link InMemorySink} sinks.
 */
public class InMemorySinkAccessor {
    private final Map<String, List<Record<Event>>> recordsMap = new HashMap<>();
    private final Lock lock = new ReentrantLock();
    private boolean result = true;

    /**
     * Gets the records from an in_memory sink. This will not remove any records from
     * the sink's memory store.
     * @param testingKey The key used to identify the in_memory sink.
     * @return The records output to the sink.
     */
    public List<Record<Event>> get(final String testingKey) {
        lock.lock();
        try {
            return recordsMap.getOrDefault(testingKey, Collections.emptyList());
        } finally {
            lock.unlock();
        }
    }

    /**
     * Gets the records from an in_memory sink. Then clears those records from
     * the sink's memory store.
     * @param testingKey The key used to identify the in_memory sink.
     * @return The records output to the sink.
     */
    public List<Record<Event>> getAndClear(final String testingKey) {
        lock.lock();
        try {
            final List<Record<Event>> records = recordsMap.getOrDefault(testingKey, Collections.emptyList());

            recordsMap.remove(testingKey);

            return records;
        } finally {
            lock.unlock();
        }
    }

    void addEvents(final String testingKey, final Collection<Record<Event>> recordsToAdd) {
        lock.lock();
        try {
            recordsMap.computeIfAbsent(testingKey, i -> new ArrayList<>())
                    .addAll(recordsToAdd);
        } finally {
            lock.unlock();
        }
    }

    public void setResult(boolean value) {
        result = value;
    }

    public boolean getResult() {
        return result;
    }
}
