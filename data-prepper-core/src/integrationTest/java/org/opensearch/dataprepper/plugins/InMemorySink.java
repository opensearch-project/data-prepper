/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.Sink;

import java.util.Collection;

@DataPrepperPlugin(name = "in_memory", pluginType = Sink.class, pluginConfigurationType = InMemoryConfig.class)
public class InMemorySink implements Sink<Record<Event>> {

    private final String testingKey;
    private final InMemorySinkAccessor inMemorySinkAccessor;

    @DataPrepperPluginConstructor
    public InMemorySink(final InMemoryConfig inMemoryConfig,
                        final InMemorySinkAccessor inMemorySinkAccessor) {
        testingKey = inMemoryConfig.getTestingKey();
        this.inMemorySinkAccessor = inMemorySinkAccessor;
    }

    @Override
    public void output(final Collection<Record<Event>> records) {
        inMemorySinkAccessor.addEvents(testingKey, records);
    }

    @Override
    public void shutdown() {

    }
}
