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

@DataPrepperPlugin(name = "mutate_event", pluginType = Processor.class, pluginConfigurationType = MutateEventProcessorConfig.class)
public class MutateEventProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {

    private final MutateEventProcessorConfig mutateEventProcessorConfig;
    @DataPrepperPluginConstructor
    public MutateEventProcessor(final PluginMetrics pluginMetrics, final MutateEventProcessorConfig mutateGenericProcessorConfig) {
        super(pluginMetrics);
        this.mutateEventProcessorConfig = mutateGenericProcessorConfig;
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for(final Record<Event> record : records) {
            final Event recordEvent = record.getData();

            if (mutateEventProcessorConfig.getRename() != null) {
                final String key = mutateEventProcessorConfig.getRename().keySet().toArray()[0].toString();
                final String newKey = mutateEventProcessorConfig.getRename().values().toArray()[0].toString();

                if(key != null
                        && newKey != null
                        && !key.equals(newKey)
                        && (!recordEvent.containsKey(newKey) || mutateEventProcessorConfig.getOverwrite())) {
                    final Object source = recordEvent.get(key, Object.class);
                    recordEvent.put(newKey, source);
                    recordEvent.delete(key);
                }
            } else if(mutateEventProcessorConfig.getAdd() != null) {
                final String key = mutateEventProcessorConfig.getAdd().keySet().toArray()[0].toString();
                final Object newValue = mutateEventProcessorConfig.getAdd().values().toArray()[0];

                if(key != null && (!recordEvent.containsKey(key) || mutateEventProcessorConfig.getOverwrite())) {
                    recordEvent.put(key, newValue);
                }
            } else if(mutateEventProcessorConfig.getDelete() != null) {
                final String key = mutateEventProcessorConfig.getDelete();

                if(key != null) {
                    recordEvent.delete(key);
                }
            } else if(mutateEventProcessorConfig.getCopy() != null) {
                final String key = mutateEventProcessorConfig.getCopy().keySet().toArray()[0].toString();
                final String newKey = mutateEventProcessorConfig.getCopy().values().toArray()[0].toString();
                if(key != null
                        && newKey != null
                        && !key.equals(newKey)
                        && (!recordEvent.containsKey(newKey) || mutateEventProcessorConfig.getOverwrite())) {
                    final Object source = recordEvent.get(key, Object.class);
                    recordEvent.put(newKey, source);
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
