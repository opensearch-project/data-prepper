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

/**
 * This processor takes in a key and truncates its value to a string with
 * characters from the front or at the end or at both removed.
 * If the value is not a string, no action is performed.
 */
@DataPrepperPlugin(name = "truncate_string", pluginType = Processor.class, pluginConfigurationType = TruncateStringProcessorConfig.class)
public class TruncateStringProcessor extends AbstractStringProcessor<TruncateStringProcessorConfig.Entry> {
    private final ExpressionEvaluator expressionEvaluator;

    @DataPrepperPluginConstructor
    public TruncateStringProcessor(final PluginMetrics pluginMetrics, final TruncateStringProcessorConfig config, final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics, config);
        this.expressionEvaluator = expressionEvaluator;
    }

    @Override
    protected void performKeyAction(final Event recordEvent, final TruncateStringProcessorConfig.Entry entry, final String value) {
        if (entry.getTruncateWhen() != null && !expressionEvaluator.evaluateConditional(entry.getTruncateWhen(), recordEvent)) {
            return;
        }
        int startIndex = entry.getStartAt() == null ? 0 : entry.getStartAt();
        Integer length = entry.getLength();
        String truncatedValue = (length == null || startIndex+length >= value.length()) ? value.substring(startIndex) : value.substring(startIndex, startIndex + length);

        recordEvent.put(entry.getSource(), truncatedValue);
    }

    @Override
    protected String getKey(final TruncateStringProcessorConfig.Entry entry) {
        return entry.getSource();
    }
}

