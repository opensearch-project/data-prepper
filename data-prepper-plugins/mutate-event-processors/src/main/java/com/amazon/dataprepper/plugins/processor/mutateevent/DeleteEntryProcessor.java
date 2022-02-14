/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.mutateevent;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.annotations.DataPrepperPluginConstructor;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.processor.AbstractProcessor;
import com.amazon.dataprepper.model.processor.Processor;
import com.amazon.dataprepper.model.record.Record;

import java.util.Collection;

@DataPrepperPlugin(name = "delete_entries", pluginType = Processor.class, pluginConfigurationType = DeleteEntryProcessorConfig.class)
public class DeleteEntryProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private final String[] entries;

    @DataPrepperPluginConstructor
    public DeleteEntryProcessor(final PluginMetrics pluginMetrics, final DeleteEntryProcessorConfig config) {
        super(pluginMetrics);
        this.entries = config.getWithKeys();
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for(final Record<Event> record : records) {
            final Event recordEvent = record.getData();

            for(String entry : entries) {
                recordEvent.delete(entry);
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
