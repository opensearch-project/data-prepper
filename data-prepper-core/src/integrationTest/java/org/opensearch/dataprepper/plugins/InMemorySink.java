/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.model.event.EventHandle;

import java.util.Collection;

@DataPrepperPlugin(name = "in_memory", pluginType = Sink.class, pluginConfigurationType = InMemoryConfig.class)
public class InMemorySink implements Sink<Record<Event>> {

    private final String testingKey;
    private final InMemorySinkAccessor inMemorySinkAccessor;
    private final AcknowledgementSetManager acknowledgementSetManager;

    @DataPrepperPluginConstructor
    public InMemorySink(final InMemoryConfig inMemoryConfig,
                        final AcknowledgementSetManager acknowledgementSetManager,
                        final InMemorySinkAccessor inMemorySinkAccessor) {
        testingKey = inMemoryConfig.getTestingKey();
        this.inMemorySinkAccessor = inMemorySinkAccessor;
        this.acknowledgementSetManager = acknowledgementSetManager;
    }

    @Override
    public void output(final Collection<Record<Event>> records) {
        inMemorySinkAccessor.addEvents(testingKey, records);
        boolean result = inMemorySinkAccessor.getResult();
        records.stream().forEach((record) -> {
            EventHandle eventHandle = ((Event)record.getData()).getEventHandle();
            acknowledgementSetManager.releaseEventReference(eventHandle, result);
        });
    }

    @Override
    public void shutdown() {

    }

    @Override
    public void initialize() {

    }

    @Override
    public boolean isReady() {
        return true;
    }
}
