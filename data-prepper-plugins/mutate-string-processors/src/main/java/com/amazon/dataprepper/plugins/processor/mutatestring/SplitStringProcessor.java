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
import java.util.regex.PatternSyntaxException;

@DataPrepperPlugin(name = "split_string", pluginType = Processor.class, pluginConfigurationType = SplitStringProcessorConfig.class)
public class SplitStringProcessor extends AbstractStringProcessor<SplitStringProcessorConfig.Entry> {

    private final Map<String, Pattern> patternMap = new HashMap<>();

    @DataPrepperPluginConstructor
    public SplitStringProcessor(final PluginMetrics pluginMetrics, final SplitStringProcessorConfig config) {
        super(pluginMetrics, config);

        for (SplitStringProcessorConfig.Entry entry: config.getEntries()) {
            if(entry.getDelimiterRegex() != null && !entry.getDelimiterRegex().isEmpty()
                    && entry.getDelimiter() != null && !entry.getDelimiter().isEmpty()) {
                throw new IllegalArgumentException("delimiter and delimiter_regex cannot be defined at the same time");
            } else if((entry.getDelimiterRegex() == null || entry.getDelimiterRegex().isEmpty()) &&
                    (entry.getDelimiter() == null || entry.getDelimiter().isEmpty())) {
                throw new IllegalArgumentException("delimiter or delimiter_regex needs to be defined");
            }

            if(entry.getDelimiterRegex() != null && !entry.getDelimiterRegex().isEmpty()) {
                patternMap.put(entry.getDelimiterRegex(), Pattern.compile(entry.getDelimiterRegex()));
            } else {
                if(isRegexChar(entry.getDelimiter())) {
                    patternMap.put(entry.getDelimiter(), Pattern.compile("\\" + entry.getDelimiter()));
                } else {
                    patternMap.put(entry.getDelimiter(), Pattern.compile(entry.getDelimiter()));
                }
            }
        }
    }

    private boolean isRegexChar(final String str) {
        try {
            Pattern.compile(str);
            return false;
        } catch(PatternSyntaxException e) {
            return true;
        }
    }

    @Override
    protected void performKeyAction(final Event recordEvent, final SplitStringProcessorConfig.Entry entry, final String value) {
        final String lookup;
        if(entry.getDelimiterRegex() != null && !entry.getDelimiterRegex().isEmpty()) {
            lookup = entry.getDelimiterRegex();
        } else {
            lookup = entry.getDelimiter();
        }

        final Pattern pattern = patternMap.get(lookup);
        final String[] splitValue = pattern.split(value);
        recordEvent.put(entry.getSource(), splitValue);
    }

    @Override
    protected String getKey(final SplitStringProcessorConfig.Entry entry) {
        return entry.getSource();
    }

}
