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

@DataPrepperPlugin(name = "copy_value", pluginType = Processor.class, pluginConfigurationType = CopyValueProcessorConfig.class)
public class CopyValueProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private final String key;
    private final String newKey;
    private final boolean overwriteIfToKeyExists;

    @DataPrepperPluginConstructor
    public CopyValueProcessor(final PluginMetrics pluginMetrics, final CopyValueProcessorConfig config) {
        super(pluginMetrics);
        this.key = config.getFromKey();
        this.newKey = config.getToKey();
        this.overwriteIfToKeyExists = config.getOverwriteIfToKeyExists();
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        if (key.equals(newKey)) {
            return records;
        }

        for(final Record<Event> record : records) {
            final Event recordEvent = record.getData();
            if(!recordEvent.containsKey(newKey) || overwriteIfToKeyExists) {
                final Object source = recordEvent.get(key, Object.class);
                recordEvent.put(newKey, source);
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
