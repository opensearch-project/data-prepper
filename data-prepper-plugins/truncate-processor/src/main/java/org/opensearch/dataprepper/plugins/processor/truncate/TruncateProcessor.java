/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.truncate;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;

import java.util.Collection;
import java.util.ArrayList;
import java.util.List;

/**
 * This processor takes in a key and truncates its value to a string with
 * characters from the front or at the end or at both removed.
 * If the value is not a string, no action is performed.
 */
@DataPrepperPlugin(name = "truncate", pluginType = Processor.class, pluginConfigurationType = TruncateProcessorConfig.class)
public class TruncateProcessor extends AbstractProcessor<Record<Event>, Record<Event>>{
    private final ExpressionEvaluator expressionEvaluator;
    private final String truncateWhen;
    private final int startIndex;
    private final Integer length;
    private final String source;

    @DataPrepperPluginConstructor
    public TruncateProcessor(final PluginMetrics pluginMetrics, final TruncateProcessorConfig config, final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.expressionEvaluator = expressionEvaluator;
        this.truncateWhen = config.getTruncateWhen();
        this.source = config.getSource();
        this.startIndex = config.getStartAt() == null ? 0 : config.getStartAt();
        this.length = config.getLength();
    }

    private String getTruncatedValue(final String value) {
        String truncatedValue = 
            (length == null || startIndex+length >= value.length()) ? 
            value.substring(startIndex) : 
            value.substring(startIndex, startIndex + length);

        return truncatedValue;
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for(final Record<Event> record : records) {
            final Event recordEvent = record.getData();
            if (truncateWhen != null && !expressionEvaluator.evaluateConditional(truncateWhen, recordEvent)) {
                continue;
            }
            if (!recordEvent.containsKey(source)) {
                continue;
            }

            final Object value = recordEvent.get(source, Object.class);
            if (value instanceof String) {
                recordEvent.put(source, getTruncatedValue((String)value));
            } else if (value instanceof List) {
                List<Object> result = new ArrayList<>();
                for (Object listItem: (List)value) {
                    if (listItem instanceof String) {
                        result.add(getTruncatedValue((String)listItem));
                    } else {
                        result.add(listItem);
                    }
                }
                recordEvent.put(source, result);
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

