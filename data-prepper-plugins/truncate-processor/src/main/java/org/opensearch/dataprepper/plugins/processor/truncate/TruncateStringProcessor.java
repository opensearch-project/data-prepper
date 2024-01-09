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
@DataPrepperPlugin(name = "truncate", pluginType = Processor.class, pluginConfigurationType = TruncateStringProcessorConfig.class)
public class TruncateStringProcessor extends AbstractProcessor<Record<Event>, Record<Event>>{
    private final List<TruncateStringProcessorConfig.Entry> entries;
    private final ExpressionEvaluator expressionEvaluator;

    @DataPrepperPluginConstructor
    public TruncateStringProcessor(final PluginMetrics pluginMetrics, final TruncateStringProcessorConfig config, final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.entries = config.getEntries();
        this.expressionEvaluator = expressionEvaluator;
    }

    private String getTruncatedValue(final TruncateStringProcessorConfig.Entry entry, final String value) {
        int startIndex = entry.getStartAt() == null ? 0 : entry.getStartAt();
        Integer length = entry.getLength();
        String truncatedValue = (length == null || startIndex+length >= value.length()) ? value.substring(startIndex) : value.substring(startIndex, startIndex + length);

        return truncatedValue;
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for(final Record<Event> record : records) {
            final Event recordEvent = record.getData();
            for(TruncateStringProcessorConfig.Entry entry : entries) {
                if (entry.getTruncateWhen() != null && !expressionEvaluator.evaluateConditional(entry.getTruncateWhen(), recordEvent)) {
                    continue;
                }
                final String key = entry.getSource();
                if (!recordEvent.containsKey(key)) {
                    continue;
                }

                final Object value = recordEvent.get(key, Object.class);
                if (value instanceof String) {
                    recordEvent.put(key, getTruncatedValue(entry, (String)value));
                } else if (value instanceof List) {
                    List<Object> result = new ArrayList<>();
                    for (Object arrayObject: (List)value) {
                        if (arrayObject instanceof String) {
                            result.add(getTruncatedValue(entry, (String)arrayObject));
                        } else {
                            result.add(arrayObject);
                        }
                    }
                    recordEvent.put(key, result);
                }
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

