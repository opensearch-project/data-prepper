/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.mutatefield;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.annotations.DataPrepperPluginConstructor;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.processor.AbstractProcessor;
import com.amazon.dataprepper.model.processor.Processor;
import com.amazon.dataprepper.model.record.Record;

import java.util.Collection;

@DataPrepperPlugin(name = "mutate_field", pluginType = Processor.class, pluginConfigurationType = MutateFieldProcessorConfig.class)
public class MutateFieldProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {

    private final MutateFieldProcessorConfig mutateGenericProcessorConfig;
    @DataPrepperPluginConstructor
    public MutateFieldProcessor(final PluginMetrics pluginMetrics, final MutateFieldProcessorConfig mutateGenericProcessorConfig) {
        super(pluginMetrics);
        this.mutateGenericProcessorConfig = mutateGenericProcessorConfig;
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for(final Record<Event> record : records) {
            final Event recordEvent = record.getData();

            if (mutateGenericProcessorConfig.getRename() != null) {
                final String key = mutateGenericProcessorConfig.getRename().keySet().toArray()[0].toString();
                final String newKey = mutateGenericProcessorConfig.getRename().values().toArray()[0].toString();

                if(key != null
                        && newKey != null
                        && !key.equals(newKey)) {
                    final Object source = recordEvent.get(key, Object.class);
                    recordEvent.put(newKey, source);
                    recordEvent.delete(key);
                }
            } else if(mutateGenericProcessorConfig.getAdd() != null) {
                final String key = mutateGenericProcessorConfig.getAdd().keySet().toArray()[0].toString();
                final Object newValue = mutateGenericProcessorConfig.getAdd().values().toArray()[0];

                if(key != null) {
                    recordEvent.put(key, newValue);
                }
            } else if(mutateGenericProcessorConfig.getDelete() != null) {
                final String key = mutateGenericProcessorConfig.getDelete();

                if(key != null) {
                    recordEvent.delete(key);
                }
            } else if(mutateGenericProcessorConfig.getCopy() != null) {
                final String key = mutateGenericProcessorConfig.getCopy().keySet().toArray()[0].toString();
                final String newKey = mutateGenericProcessorConfig.getCopy().values().toArray()[0].toString();
                if(key != null
                        && newKey != null
                        && !key.equals(newKey)) {
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
