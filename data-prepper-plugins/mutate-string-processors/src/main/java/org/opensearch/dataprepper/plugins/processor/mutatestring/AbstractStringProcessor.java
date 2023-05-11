/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutatestring;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collection;
import java.util.List;

public abstract class AbstractStringProcessor<T> extends AbstractProcessor<Record<Event>, Record<Event>> {
    private final List<T> entries;

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
                    performKeyAction(recordEvent, entry, (String) value);
                }
            }
        }
    }

    protected abstract void performKeyAction(final Event recordEvent, final T entry, final String value);

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
