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
import org.opensearch.dataprepper.model.processor.Processor;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This processor takes in a key and changes its value by searching for a pattern and replacing the matches with a string.
 * If the value is not a string, no action is performed.
 */
@DataPrepperPlugin(name = "substitute_string", pluginType = Processor.class, pluginConfigurationType = SubstituteStringProcessorConfig.class)
public class SubstituteStringProcessor extends AbstractStringProcessor<SubstituteStringProcessorConfig.Entry> {
    private final Map<String, Pattern> patternMap = new HashMap<>();
    private final ExpressionEvaluator<Boolean> expressionEvaluator;

    @DataPrepperPluginConstructor
    public SubstituteStringProcessor(final PluginMetrics pluginMetrics, final SubstituteStringProcessorConfig config, final ExpressionEvaluator<Boolean> expressionEvaluator) {
        super(pluginMetrics, config);
        this.expressionEvaluator = expressionEvaluator;

        for(final SubstituteStringProcessorConfig.Entry entry : config.getEntries()) {
            patternMap.put(entry.getFrom(), Pattern.compile(entry.getFrom()));
        }
    }

    @Override
    protected void performKeyAction(final Event recordEvent, final SubstituteStringProcessorConfig.Entry entry, final String value)
    {
        if (Objects.nonNull(entry.getSubstituteWhen()) && !expressionEvaluator.evaluate(entry.getSubstituteWhen(), recordEvent)) {
            return;
        }

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
