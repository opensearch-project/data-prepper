/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutatestring;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.processor.Processor;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.opensearch.dataprepper.model.pattern.Pattern;

@DataPrepperPlugin(name = "split_string", pluginType = Processor.class, pluginConfigurationType = SplitStringProcessorConfig.class)
public class SplitStringProcessor extends AbstractStringProcessor<SplitStringProcessorConfig.Entry> {

    private final Map<String, Pattern> patternMap = new HashMap<>();
    private final ExpressionEvaluator expressionEvaluator;

    @DataPrepperPluginConstructor
    public SplitStringProcessor(final PluginMetrics pluginMetrics, final SplitStringProcessorConfig config, final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics, config);
        this.expressionEvaluator = expressionEvaluator;

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
                patternMap.put(entry.getDelimiter(), Pattern.compile(Pattern.quote(entry.getDelimiter())));
            }

            if (entry.getSplitWhen() != null
                    && !expressionEvaluator.isValidExpressionStatement(entry.getSplitWhen())) {
                throw new InvalidPluginConfigurationException(
                        String.format("split_when %s is not a valid expression statement. See https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/ for valid expression syntax", entry.getSplitWhen()));
            }
        }
    }

    @Override
    protected void performKeyAction(final Event recordEvent, final SplitStringProcessorConfig.Entry entry, final String value) {
        if (Objects.nonNull(entry.getSplitWhen()) && !expressionEvaluator.evaluateConditional(entry.getSplitWhen(), recordEvent)) {
            return;
        }

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
    protected EventKey getKey(final SplitStringProcessorConfig.Entry entry) {
        return entry.getSource();
    }

}
