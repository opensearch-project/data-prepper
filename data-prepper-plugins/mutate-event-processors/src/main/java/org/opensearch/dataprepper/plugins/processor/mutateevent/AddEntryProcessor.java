/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collection;
import java.util.List;

@DataPrepperPlugin(name = "add_entries", pluginType = Processor.class, pluginConfigurationType = AddEntryProcessorConfig.class)
public class AddEntryProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private final List<AddEntryProcessorConfig.Entry> entries;

    @DataPrepperPluginConstructor
    public AddEntryProcessor(final PluginMetrics pluginMetrics, final AddEntryProcessorConfig config) {
        super(pluginMetrics);
        this.entries = config.getEntries();
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for(final Record<Event> record : records) {
            final Event recordEvent = record.getData();

            for(AddEntryProcessorConfig.Entry entry : entries) {
                if (!recordEvent.containsKey(entry.getKey()) || entry.getOverwriteIfKeyExists()) {
                    recordEvent.put(entry.getKey(), entry.getValue());
                }
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
