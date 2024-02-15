/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.flattenjson;

import com.github.wnameless.json.flattener.JsonFlattener;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@DataPrepperPlugin(name = "flatten", pluginType = Processor.class, pluginConfigurationType = FlattenJsonProcessorConfig.class)
public class FlattenJsonProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(FlattenJsonProcessor.class);

    private static final String SEPARATOR = "/";
    private final FlattenJsonProcessorConfig config;
    private final ExpressionEvaluator expressionEvaluator;

    @DataPrepperPluginConstructor
    public FlattenJsonProcessor(final PluginMetrics pluginMetrics, final FlattenJsonProcessorConfig config, final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.config = config;
        this.expressionEvaluator = expressionEvaluator;
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for (final Record<Event> record : records) {
            final Event recordEvent = record.getData();

            try {
                if (config.getFlattenWhen() != null && !expressionEvaluator.evaluateConditional(config.getFlattenWhen(), recordEvent)) {
                    continue;
                }

                final String sourceJson = recordEvent.getAsJsonString(config.getSource());

                Map<String, Object> flattenedJson = new JsonFlattener(sourceJson).ignoreReservedCharacters().flattenAsMap();

                if (config.isRemoveProcessedFields()) {
                    final Map<String, Object> sourceMap = recordEvent.get(config.getSource(), Map.class);
                    for (final String keyInSource : sourceMap.keySet()) {
                        recordEvent.delete(getJsonPointer(config.getSource(), keyInSource));
                    }
                }

                if (config.isRemoveListIndices()) {
                    flattenedJson = removeListIndicesInKey(flattenedJson);
                }

                updateEvent(recordEvent, flattenedJson);
            } catch (Exception e) {
                LOG.error("Fail to perform flatten operation", e);
                recordEvent.getMetadata().addTags(config.getTagsOnFailure());
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

    private String getJsonPointer(final String outerKey, final String innerKey) {
        if (outerKey.isEmpty()) {
            return SEPARATOR + innerKey;
        } else {
            return SEPARATOR + outerKey + SEPARATOR + innerKey;
        }
    }

    private Map<String, Object> removeListIndicesInKey(final Map<String, Object> inputMap) {
        final Map<String, Object> resultMap = new HashMap<>();

        for (final Map.Entry<String, Object> entry : inputMap.entrySet()) {
            final String keyWithoutIndices = removeListIndices(entry.getKey());
            addFieldsToMapWithMerge(keyWithoutIndices, entry.getValue(), resultMap);
        }
        return resultMap;
    }

    private String removeListIndices(final String key) {
        return key.replaceAll("\\[\\d+\\]", "[]");
    }

    private void addFieldsToMapWithMerge(String key, Object value, Map<String, Object> map) {
        if (!map.containsKey(key)) {
            map.put(key, value);
        } else {
            final Object currentValue = map.get(key);
            if (currentValue instanceof List) {
                ((List<Object>)currentValue).add(value);
            } else {
                List<Object> newValue = new ArrayList<>();
                newValue.add(currentValue);
                newValue.add(value);
                map.put(key, newValue);
            }
        }
    }

    private void updateEvent(Event recordEvent, Map<String, Object> flattenedJson) {
        if (config.getTarget().isEmpty()) {
            // Target is root
            for (final Map.Entry<String, Object> entry : flattenedJson.entrySet()) {
                recordEvent.put(entry.getKey(), entry.getValue());
            }
        } else {
            recordEvent.put(config.getTarget(), flattenedJson);
        }
    }
}
