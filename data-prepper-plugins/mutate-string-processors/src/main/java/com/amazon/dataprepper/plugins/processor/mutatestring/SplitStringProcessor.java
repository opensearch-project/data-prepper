/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.mutatestring;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.annotations.DataPrepperPluginConstructor;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.processor.Processor;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

@DataPrepperPlugin(name = "split_string", pluginType = Processor.class, pluginConfigurationType = SplitStringProcessorConfig.class)
public class SplitStringProcessor extends AbstractStringProcessor<SplitStringProcessorConfig.Entry> {

    private final Map<String, Pattern> patternMap = new HashMap<>();

    @DataPrepperPluginConstructor
    public SplitStringProcessor(final PluginMetrics pluginMetrics, final SplitStringProcessorConfig config) {
        super(pluginMetrics, config);

        for (SplitStringProcessorConfig.Entry entry: config.getEntries()) {
            patternMap.put(entry.getDelimiter(), Pattern.compile(entry.getDelimiter()));
        }
    }

    @Override
    protected void performKeyAction(final Event recordEvent, final SplitStringProcessorConfig.Entry entry, final String value) {

        final Pattern pattern = patternMap.get(entry.getDelimiter());
        final String[] splitValue = pattern.split(value);
        recordEvent.put(entry.getSource(), splitValue);
    }

    @Override
    protected String getKey(final SplitStringProcessorConfig.Entry entry) {
        return entry.getSource();
    }

}
