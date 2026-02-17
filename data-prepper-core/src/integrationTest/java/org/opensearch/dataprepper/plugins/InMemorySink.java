/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.Sink;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.model.sink.SinkForwardRecordsContext;

import java.util.Collection;

@DataPrepperPlugin(name = "in_memory", pluginType = Sink.class, pluginConfigurationType = InMemoryConfig.class)
public class InMemorySink implements Sink<Record<Event>> {

    private final String testingKey;
    private final InMemorySinkAccessor inMemorySinkAccessor;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final Boolean acknowledgements;
    private final SinkContext sinkContext;

    @DataPrepperPluginConstructor
    public InMemorySink(final InMemoryConfig inMemoryConfig,
                        final AcknowledgementSetManager acknowledgementSetManager,
                        final SinkContext sinkContext,
                        final InMemorySinkAccessor inMemorySinkAccessor) {
        testingKey = inMemoryConfig.getTestingKey();
        this.sinkContext = sinkContext;
        this.inMemorySinkAccessor = inMemorySinkAccessor;
        this.acknowledgementSetManager = acknowledgementSetManager;
        acknowledgements = inMemoryConfig.getAcknowledgements();
    }

    @Override
    public void output(final Collection<Record<Event>> records) {
        inMemorySinkAccessor.addEvents(testingKey, records);
        boolean result = inMemorySinkAccessor.getResult();
        SinkForwardRecordsContext sinkForwardRecordsContext = new SinkForwardRecordsContext(sinkContext);
        sinkForwardRecordsContext.addRecords(records);
        records.stream().forEach((record) -> {
            EventHandle eventHandle = ((Event)record.getData()).getEventHandle();
            if (acknowledgements) {
                eventHandle.release(result);
            }
        });
        sinkContext.forwardRecords(sinkForwardRecordsContext, null, null);
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
