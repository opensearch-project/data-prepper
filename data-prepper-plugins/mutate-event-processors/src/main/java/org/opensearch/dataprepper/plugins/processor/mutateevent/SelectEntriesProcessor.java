/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.opensearch.dataprepper.model.pattern.Pattern;

@DataPrepperPlugin(name = "select_entries", pluginType = Processor.class, pluginConfigurationType = SelectEntriesProcessorConfig.class)
public class SelectEntriesProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private final List<String> keysToInclude;
    private final List<Pattern> includeKeysRegex;
    private final String includeKeysRegexPointer;
    private final String selectWhen;

    private final ExpressionEvaluator expressionEvaluator;

    @DataPrepperPluginConstructor
    public SelectEntriesProcessor(final PluginMetrics pluginMetrics, final SelectEntriesProcessorConfig config, final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.selectWhen = config.getSelectWhen();
        if (selectWhen != null
                && !expressionEvaluator.isValidExpressionStatement(selectWhen)) {
            throw new InvalidPluginConfigurationException(
                    String.format("select_when value of %s is not a valid expression statement. " +
                            "See https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/ for valid expression syntax.", selectWhen));
        }
        this.keysToInclude = config.getIncludeKeys();
        this.includeKeysRegex = config.getIncludeKeysRegex();
        this.includeKeysRegexPointer = config.getIncludeKeysRegexPointer();
        this.expressionEvaluator = expressionEvaluator;
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for(final Record<Event> record : records) {
            final Event recordEvent = record.getData();

            if (Objects.nonNull(selectWhen) && !expressionEvaluator.evaluateConditional(selectWhen, recordEvent)) {
                continue;
            }
            // To handle nested case, just get the values and store
            // in a temporary map.
            Map<String, Object> outMap = getIncludeKeysOutputMap(recordEvent);

            Map<String, Object> regexOutMap = getIncludeKeysRegexOutputMap(recordEvent);

            recordEvent.clear();
    
            // add back only the keys selected
            for (Map.Entry<String, Object> entry: outMap.entrySet()) {
                recordEvent.put(entry.getKey(), entry.getValue());
            }

            for (Map.Entry<String, Object> entry: regexOutMap.entrySet()) {
                recordEvent.put(entry.getKey(), entry.getValue());
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

    private Map<String, Object> getIncludeKeysOutputMap(final Event event) {
        Map<String, Object> outMap = new HashMap<>();
        if (keysToInclude != null) {
            for (String keyToInclude: keysToInclude) {
                Object value = event.get(keyToInclude, Object.class);
                if (value != null) {
                    outMap.put(keyToInclude, value);
                }
            }
        }

        return outMap;
    }

    private Map<String, Object> getIncludeKeysRegexOutputMap(final Event event) {
        if (includeKeysRegex == null || includeKeysRegex.isEmpty()) {
            return Collections.emptyMap();
        }

        final Map<String, Object> outputMap = new HashMap<>();

        Map<String, Object> eventMap;

        if (includeKeysRegexPointer != null && !includeKeysRegexPointer.equals("/")) {
            if (!event.containsKey(includeKeysRegexPointer)) {
                return Collections.emptyMap();
            }

            eventMap = event.get(includeKeysRegexPointer, Map.class);
        } else {
            eventMap = event.toMap();
        }

        for (final Map.Entry<String, Object> entry : eventMap.entrySet()) {
            if (keysToInclude != null && keysToInclude.contains(entry.getKey())) {
                continue;
            }

            for (final Pattern includeKeyRegex : includeKeysRegex) {
                if (includeKeyRegex.matcher(entry.getKey()).matches()) {
                    final String fullKey = getFullKey(entry.getKey(), includeKeysRegexPointer);
                    outputMap.put(fullKey, entry.getValue());
                    break;
                }
            }
        }

        return outputMap;
    }

    private String getFullKey(final String key, final String includeKeysRegexPointer) {
        return includeKeysRegexPointer != null ? includeKeysRegexPointer + "/" + key : key;
    }
}

