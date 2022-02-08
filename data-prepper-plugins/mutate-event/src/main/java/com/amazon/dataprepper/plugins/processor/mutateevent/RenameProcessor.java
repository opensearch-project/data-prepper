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

@DataPrepperPlugin(name = "rename_entry", pluginType = Processor.class, pluginConfigurationType = RenameProcessorConfig.class)
public class RenameProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private final RenameProcessorConfig config;

    @DataPrepperPluginConstructor
    public RenameProcessor(final PluginMetrics pluginMetrics, final RenameProcessorConfig config) {
        super(pluginMetrics);
        this.config = config;
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for(final Record<Event> record : records) {
            final Event recordEvent = record.getData();
            final String key = config.getFrom();
            final String newKey = config.getTo();

            if(key != null
                    && newKey != null
                    && !key.equals(newKey)
                    && (!recordEvent.containsKey(newKey) || !config.getSkipIfPresent())) {
                final Object source = recordEvent.get(key, Object.class);
                recordEvent.put(newKey, source);
                recordEvent.delete(key);
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
