/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collection;

/**
 * Returns what it receives, but also adds a key "thread_name" with
 * a value set to the name of the current thread.
 */
@DataPrepperPlugin(name = "thread_info", pluginType = Processor.class)
public class ThreadInfoProcessor implements Processor<Record<Event>, Record<Event>> {

    @Override
    public Collection<Record<Event>> execute(final Collection<Record<Event>> records) {
        final String threadName = Thread.currentThread().getName();
        records.stream().map(Record::getData)
                .forEach(event -> event.put("thread_name", threadName));

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
