/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.truncate;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.EVENT;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * This processor takes in a key and truncates its value to a string with
 * characters from the front or at the end or at both removed.
 * If the value is not a string, no action is performed.
 */
@DataPrepperPlugin(name = "truncate", pluginType = Processor.class, pluginConfigurationType = TruncateProcessorConfig.class)
public class TruncateProcessor extends AbstractProcessor<Record<Event>, Record<Event>>{
    private static final Logger LOG = LoggerFactory.getLogger(TruncateProcessor.class);
    private final ExpressionEvaluator expressionEvaluator;
    private final List<TruncateProcessorConfig.Entry> entries;

    @DataPrepperPluginConstructor
    public TruncateProcessor(final PluginMetrics pluginMetrics, final TruncateProcessorConfig config, final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.expressionEvaluator = expressionEvaluator;
        this.entries = config.getEntries();

        config.getEntries().forEach(entry -> {
            if (entry.getTruncateWhen() != null
                    && !expressionEvaluator.isValidExpressionStatement(entry.getTruncateWhen())) {
                throw new InvalidPluginConfigurationException(
                        String.format("truncate_when %s is not a valid expression statement. See https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/ for valid expression syntax", entry.getTruncateWhen()));
            }
        });
    }

    private String getTruncatedValue(final String value, final int startIndex, final Integer length) {
        String truncatedValue =
            (length == null || startIndex+length >= value.length()) ?
            value.substring(startIndex) :
            value.substring(startIndex, startIndex + length);

        return truncatedValue;
    }

    private void truncateKey(Event event, String key, Object value, TruncateProcessorConfig.Entry entryConfig) {
        final boolean recurse = entryConfig.getRecurse();
        final int startIndex = entryConfig.getStartAt() == null ? 0 : entryConfig.getStartAt();
        final Integer length = entryConfig.getLength();
        if (value instanceof String) {
            event.put(key, getTruncatedValue((String) value, startIndex, length));
        } else if (value instanceof List) {
            List<Object> result = new ArrayList<>();
            for (Object listItem : (List) value) {
                if (listItem instanceof String) {
                    result.add(getTruncatedValue((String) listItem, startIndex, length));
                } else {
                    result.add(listItem);
                }
            }
            event.put(key, result);
        } else if (recurse && (value instanceof Map)) {
            Map<String, Object> valueMap = (Map<String, Object>)value;
            for (Map.Entry<String, Object> mapEntry: valueMap.entrySet()) {
                truncateKey(event, key+"/"+mapEntry.getKey(), mapEntry.getValue(), entryConfig);
            }
        }
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for(final Record<Event> record : records) {
            final Event recordEvent = record.getData();

            try {
                for (TruncateProcessorConfig.Entry entry : entries) {
                    final List<String> sourceKeys = entry.getSourceKeys();
                    final String truncateWhen = entry.getTruncateWhen();
                    final boolean recurse = entry.getRecurse();
                    final int startIndex = entry.getStartAt() == null ? 0 : entry.getStartAt();
                    final Integer length = entry.getLength();
                    if (truncateWhen != null && !expressionEvaluator.evaluateConditional(truncateWhen, recordEvent)) {
                        continue;
                    }
                    if (sourceKeys == null) {
                        for (Map.Entry<String, Object> mapEntry: recordEvent.toMap().entrySet()) {
                            truncateKey(recordEvent, mapEntry.getKey(), mapEntry.getValue(), entry);
                        }
                        continue;
                    }

                    for (String sourceKey : sourceKeys) {
                        if (!recordEvent.containsKey(sourceKey)) {
                            continue;
                        }

                        final Object value = recordEvent.get(sourceKey, Object.class);
                        truncateKey(recordEvent, sourceKey, value, entry);
                    }
                }
            } catch (final Exception e) {
                LOG.atError()
                        .addMarker(EVENT)
                        .addMarker(NOISY)
                        .setMessage("There was an exception while processing Event [{}]")
                        .addArgument(recordEvent)
                        .setCause(e)
                        .log();
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

