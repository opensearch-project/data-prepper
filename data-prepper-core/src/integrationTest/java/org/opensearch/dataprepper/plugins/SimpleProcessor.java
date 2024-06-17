/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyFactory;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collection;

@DataPrepperPlugin(name = "simple_test", pluginType = Processor.class, pluginConfigurationType = SimpleProcessorConfig.class)
public class SimpleProcessor implements Processor<Record<Event>, Record<Event>> {
    private final EventKey eventKey1;
    private final String valuePrefix1;
    int count = 0;

    @DataPrepperPluginConstructor
    public SimpleProcessor(
            final SimpleProcessorConfig simpleProcessorConfig,
            final EventKeyFactory eventKeyFactory) {
        eventKey1 = eventKeyFactory.createEventKey(simpleProcessorConfig.getKey1());
        valuePrefix1 = simpleProcessorConfig.getValuePrefix1();
    }

    @Override
    public Collection<Record<Event>> execute(final Collection<Record<Event>> records) {
        for (final Record<Event> record : records) {
            record.getData().put(eventKey1, valuePrefix1 + count);
            count++;
        }

        return records;
    }

    @Override
    public void prepareForShutdown() {

    }

    @Override
    public boolean isReadyForShutdown() {
        return false;
    }

    @Override
    public void shutdown() {

    }
}
