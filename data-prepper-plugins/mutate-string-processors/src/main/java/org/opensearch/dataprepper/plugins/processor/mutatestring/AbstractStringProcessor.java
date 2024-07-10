/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutatestring;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.record.Record;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.EVENT;

public abstract class AbstractStringProcessor<T> extends AbstractProcessor<Record<Event>, Record<Event>> {
    private final List<T> entries;
    static final Logger LOG = LoggerFactory.getLogger(AbstractStringProcessor.class);

    @DataPrepperPluginConstructor
    public AbstractStringProcessor(final PluginMetrics pluginMetrics, final StringProcessorConfig<T> config) {
        super(pluginMetrics);
        this.entries = config.getIterativeConfig();
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for(final Record<Event> record : records) {
            final Event recordEvent = record.getData();
            try {
                performStringAction(recordEvent);
            } catch (final Exception e) {
                LOG.error(EVENT, "There was an exception while processing Event [{}]", recordEvent, e);
            }
        }

        return records;
    }

    private void performStringAction(final Event recordEvent)
    {
        try {
            for(final T entry : entries) {
                final EventKey key = getKey(entry);

                if(recordEvent.containsKey(key)) {
                    final Object value = recordEvent.get(key, Object.class);

                    if(value instanceof String) {
                        performKeyAction(recordEvent, entry, (String) value);
                    }
                }
            }
        } catch (Exception e) {
            LOG.error(EVENT, "Exception while performing String action", e);
        }
    }

    protected abstract void performKeyAction(final Event recordEvent, final T entry, final String value);

    protected abstract EventKey getKey(final T entry);

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
