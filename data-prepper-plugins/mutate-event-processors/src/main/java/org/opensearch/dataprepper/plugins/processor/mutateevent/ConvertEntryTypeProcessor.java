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
import org.opensearch.dataprepper.typeconverter.TypeConverter;

import java.util.Collection;

@DataPrepperPlugin(name = "convert_entry_type", pluginType = Processor.class, pluginConfigurationType = ConvertEntryTypeProcessorConfig.class)
public class ConvertEntryTypeProcessor  extends AbstractProcessor<Record<Event>, Record<Event>> {
    private final String key;
    private final TypeConverter converter;

    @DataPrepperPluginConstructor
    public ConvertEntryTypeProcessor(final PluginMetrics pluginMetrics, final ConvertEntryTypeProcessorConfig convertEntryTypeProcessorConfig) {
        super(pluginMetrics);
        this.key = convertEntryTypeProcessorConfig.getKey();
        this.converter = convertEntryTypeProcessorConfig.getType().getTargetConverter();
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for(final Record<Event> record : records) {
            final Event recordEvent = record.getData();
            Object keyVal = recordEvent.get(key, Object.class);
            if (keyVal != null) {
                recordEvent.delete(key);
                recordEvent.put(key, this.converter.convert(keyVal));
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


