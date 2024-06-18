/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collection;

@DataPrepperPlugin(name = "simple_copy_test", pluginType = Processor.class, pluginConfigurationType = SimpleCopyProcessorConfig.class)
public class SimpleCopyProcessor implements Processor<Record<Event>, Record<Event>> {
    private final SimpleCopyProcessorConfig simpleCopyProcessorConfig;
    int count = 0;

    @DataPrepperPluginConstructor
    public SimpleCopyProcessor(final SimpleCopyProcessorConfig simpleCopyProcessorConfig) {
        this.simpleCopyProcessorConfig = simpleCopyProcessorConfig;
    }

    @Override
    public Collection<Record<Event>> execute(final Collection<Record<Event>> records) {
        for (final Record<Event> record : records) {
            final Object value = record.getData().get(simpleCopyProcessorConfig.getSource(), Object.class);
            record.getData().put(simpleCopyProcessorConfig.getTarget(), value);
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
