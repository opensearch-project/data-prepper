/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.mutatestring;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.processor.Processor;

import java.util.List;

@DataPrepperPlugin(name = "split_string", pluginType = Processor.class, pluginConfigurationType = SplitStringProcessorConfig.class)
public class SplitStringProcessor extends AbstractStringProcessor<SplitStringProcessorConfig.Entry> {

    private final List<SplitStringProcessorConfig.Entry> entries;

    public SplitStringProcessor(PluginMetrics pluginMetrics, StringProcessorConfig<SplitStringProcessorConfig.Entry> config) {
        super(pluginMetrics, config);
        this.entries = config.getIterativeConfig();
    }

    @Override
    protected void performKeyAction(Event recordEvent, SplitStringProcessorConfig.Entry entry, String value) {

        String[] splitValue = value.split(entry.getDelimiter());
        recordEvent.put(entry.getSource(), splitValue);
    }

    @Override
    protected String getKey(SplitStringProcessorConfig.Entry entry) {
        return entry.getSource();
    }

}
