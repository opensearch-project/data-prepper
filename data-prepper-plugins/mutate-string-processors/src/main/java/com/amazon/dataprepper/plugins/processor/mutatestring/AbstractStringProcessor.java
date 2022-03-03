/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.mutatestring;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.annotations.DataPrepperPluginConstructor;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.processor.AbstractProcessor;
import com.amazon.dataprepper.model.record.Record;

import java.util.Collection;
import java.util.List;

public abstract class AbstractStringProcessor<T> extends AbstractProcessor<Record<Event>, Record<Event>> {
    private List<T> entries;

    @DataPrepperPluginConstructor
    public AbstractStringProcessor(final PluginMetrics pluginMetrics, final StringProcessorConfig<T> config) {
        super(pluginMetrics);
        this.entries = config.getIterativeConfig();
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for(final Record<Event> record : records) {
            final Event recordEvent = record.getData();
            performStringAction(recordEvent);
        }

        return records;
    }

    private void performStringAction(final Event recordEvent)
    {
        for(T entry : entries) {
            final String key = getKey(entry);

            if(recordEvent.containsKey(key)) {
                final Object value = recordEvent.get(key, Object.class);

                if(value instanceof String) {
                    performKeyAction(recordEvent, key, (String) value);
                }
            }
        }
    }

    protected abstract void performKeyAction(final Event recordEvent, final String key, final String value);

    protected abstract String getKey(final T entry);

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
