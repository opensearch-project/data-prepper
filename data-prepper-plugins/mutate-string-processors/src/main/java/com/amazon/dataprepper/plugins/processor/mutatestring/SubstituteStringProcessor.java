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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This processor takes in a key and changes its value by searching for a pattern and replacing the matches with a string.
 * If the value is not a string, no action is performed.
 */
@DataPrepperPlugin(name = "substitute_string", pluginType = Processor.class, pluginConfigurationType = SubstituteStringProcessorConfig.class)
public class SubstituteStringProcessor extends AbstractStringProcessor<SubstituteStringProcessorConfig.Entry> {
    private final Map<String, Pattern> patternMap = new HashMap<>();

    @DataPrepperPluginConstructor
    public SubstituteStringProcessor(final PluginMetrics pluginMetrics, final SubstituteStringProcessorConfig config) {
        super(pluginMetrics, config);

        for(final SubstituteStringProcessorConfig.Entry entry : config.getEntries()) {
            patternMap.put(entry.getFrom(), Pattern.compile(entry.getFrom()));
        }
    }

    @Override
    protected void performKeyAction(final Event recordEvent, final SubstituteStringProcessorConfig.Entry entry, final String value)
    {
        final Pattern pattern = patternMap.get(entry.getFrom());
        final Matcher matcher = pattern.matcher(value);
        final String newValue = matcher.replaceAll(entry.getTo());
        recordEvent.put(entry.getSource(), newValue);
    }

    @Override
    protected String getKey(final SubstituteStringProcessorConfig.Entry entry) {
        return entry.getSource();
    }
}
