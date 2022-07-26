/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.csv;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.annotations.DataPrepperPluginConstructor;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.processor.AbstractProcessor;
import com.amazon.dataprepper.model.processor.Processor;
import com.amazon.dataprepper.model.record.Record;
import java.util.Collection;

/**
* Processor to parse CSV data in Events.
*
* @since 2.0
 */
@DataPrepperPlugin(name="csv", pluginType = Processor.class, pluginConfigurationType = CSVProcessorConfig.class)
public class CSVProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    @DataPrepperPluginConstructor
    public CSVProcessor(final PluginMetrics pluginMetrics, final CSVProcessorConfig config) {
        super(pluginMetrics);
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
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
