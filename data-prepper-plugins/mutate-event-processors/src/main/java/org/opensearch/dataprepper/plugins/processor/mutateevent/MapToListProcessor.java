/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@DataPrepperPlugin(name = "map_to_list", pluginType = Processor.class, pluginConfigurationType = MapToListProcessorConfig.class)
public class MapToListProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(MapToListProcessor.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final MapToListProcessorConfig config;
    private final ExpressionEvaluator expressionEvaluator;
    private final Set<String> excludeKeySet = new HashSet<>();

    @DataPrepperPluginConstructor
    public MapToListProcessor(final PluginMetrics pluginMetrics, final MapToListProcessorConfig config, final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.config = config;
        this.expressionEvaluator = expressionEvaluator;
        excludeKeySet.addAll(config.getExcludeKeys());

        if (config.getMapToListWhen() != null
                && !expressionEvaluator.isValidExpressionStatement(config.getMapToListWhen())) {
            throw new InvalidPluginConfigurationException(
                    String.format("map_to_list_when %s is not a valid expression statement. See https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/ for valid expression syntax",
                            config.getMapToListWhen()));
        }
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for (final Record<Event> record : records) {
            final Event recordEvent = record.getData();

            try {

                if (config.getMapToListWhen() != null && !expressionEvaluator.evaluateConditional(config.getMapToListWhen(), recordEvent)) {
                    continue;
                }

                try {
                    final Map<String, Object> sourceMap = getSourceMap(recordEvent);

                    if (config.getConvertFieldToList()) {
                        final List<List<Object>> targetNestedList = new ArrayList<>();

                        for (final Map.Entry<String, Object> entry : sourceMap.entrySet()) {
                            if (!excludeKeySet.contains(entry.getKey())) {
                                targetNestedList.add(List.of(entry.getKey(), entry.getValue()));
                            }

                        }
                        removeProcessedFields(sourceMap, recordEvent);
                        recordEvent.put(config.getTarget(), targetNestedList);
                    } else {
                        final List<Map<String, Object>> targetList = new ArrayList<>();
                        for (final Map.Entry<String, Object> entry : sourceMap.entrySet()) {
                            if (!excludeKeySet.contains(entry.getKey())) {
                                targetList.add(Map.of(
                                        config.getKeyName(), entry.getKey(),
                                        config.getValueName(), entry.getValue()
                                ));
                            }
                        }
                        removeProcessedFields(sourceMap, recordEvent);
                        recordEvent.put(config.getTarget(), targetList);
                    }
                } catch (Exception e) {
                    LOG.error(NOISY,"Fail to perform Map to List operation", e);
                    recordEvent.getMetadata().addTags(config.getTagsOnFailure());
                }
            } catch (final Exception e) {
                LOG.error(NOISY, "There was an exception while processing Event [{}]", recordEvent, e);
                recordEvent.getMetadata().addTags(config.getTagsOnFailure());
            }
        }
        return records;
    }

    private Map<String, Object> getSourceMap(Event recordEvent) throws JsonProcessingException {
        final Map<String, Object> sourceMap;
        sourceMap = recordEvent.get(config.getSource(), Map.class);
        return sourceMap;
    }

    private void removeProcessedFields(Map<String, Object> sourceMap, Event recordEvent) {
        if (!config.getRemoveProcessedFields()) {
            return;
        }

        if (Objects.equals(config.getSource(), "")) {
            // Source is root
            for (final Map.Entry<String, Object> entry : sourceMap.entrySet()) {
                if (excludeKeySet.contains(entry.getKey())) {
                    continue;
                }
                recordEvent.delete(entry.getKey());
            }
        } else {
            Map<String, Object> modifiedSourceMap = new HashMap<>();
            for (final Map.Entry<String, Object> entry : sourceMap.entrySet()) {
                if (excludeKeySet.contains(entry.getKey())) {
                    modifiedSourceMap.put(entry.getKey(), entry.getValue());
                }
            }
            recordEvent.put(config.getSource(), modifiedSourceMap);
        }
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
