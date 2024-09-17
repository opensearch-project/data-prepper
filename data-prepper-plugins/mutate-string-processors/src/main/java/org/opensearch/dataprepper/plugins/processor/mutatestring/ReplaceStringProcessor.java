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

import java.util.Objects;

/**
 * This processor takes in a key and changes its value by replacing each occurrence of from substring to target substring.
 * If the value is not a string, no action is performed.
 */
@DataPrepperPlugin(name = "replace_string", pluginType = Processor.class, pluginConfigurationType = ReplaceStringProcessorConfig.class)
public class ReplaceStringProcessor extends AbstractStringProcessor<ReplaceStringProcessorConfig.Entry> {
    private final ExpressionEvaluator expressionEvaluator;

    @DataPrepperPluginConstructor
    public ReplaceStringProcessor(final PluginMetrics pluginMetrics, final ReplaceStringProcessorConfig config, final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics, config);
        this.expressionEvaluator = expressionEvaluator;

        for(final ReplaceStringProcessorConfig.Entry entry : config.getEntries()) {
            if (entry.getSubstituteWhen() != null
                    && !expressionEvaluator.isValidExpressionStatement(entry.getSubstituteWhen())) {
                throw new InvalidPluginConfigurationException(
                        String.format("substitute_when %s is not a valid expression statement. See https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/ for valid expression syntax", entry.getSubstituteWhen()));
            }
        }
    }

    @Override
    protected void performKeyAction(final Event recordEvent, final ReplaceStringProcessorConfig.Entry entry, final String value)
    {
        if (Objects.nonNull(entry.getSubstituteWhen()) && !expressionEvaluator.evaluateConditional(entry.getSubstituteWhen(), recordEvent)) {
            return;
        }

        final String newValue = value.replace(entry.getFrom(), entry.getTo());
        recordEvent.put(entry.getSource(), newValue);
    }

    @Override
    protected EventKey getKey(final ReplaceStringProcessorConfig.Entry entry) {
        return entry.getSource();
    }
}
