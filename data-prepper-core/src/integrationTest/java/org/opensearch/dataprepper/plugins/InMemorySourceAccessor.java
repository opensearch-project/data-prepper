/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Provides a mechanism to write records to an in_memory source. This allows the pipeline to execute
 * independently of the test code, but the test code can write to a source.
 */
public class InMemorySourceAccessor {
    private final Map<String, List<Record<Event>>> recordsMap = new HashMap<>();
    private final Lock lock = new ReentrantLock();

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
