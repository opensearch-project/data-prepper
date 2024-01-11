/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@DataPrepperPlugin(name = "map_to_list", pluginType = Processor.class, pluginConfigurationType = MapToListProcessorConfig.class)
public class MapToListProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<Map<String, Object>>() {};
    private static final Logger LOG = LoggerFactory.getLogger(MapToListProcessor.class);
    private final MapToListProcessorConfig config;
    private final ExpressionEvaluator expressionEvaluator;
    private final Set<String> excludeKeySet = new HashSet<>();

    @DataPrepperPluginConstructor
    public MapToListProcessor(final PluginMetrics pluginMetrics, final MapToListProcessorConfig config, final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.config = config;
        this.expressionEvaluator = expressionEvaluator;
        excludeKeySet.addAll(config.getExcludeKeys());
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for (final Record<Event> record : records) {
            final Event recordEvent = record.getData();

            if (config.getMapToListWhen() != null && !expressionEvaluator.evaluateConditional(config.getMapToListWhen(), recordEvent)) {
                continue;
            }

            try {
                final Map<String, Object> sourceMap = recordEvent.get(config.getSource(), Map.class);
                final List<Map<String, Object>> targetList = new ArrayList<>();

                for (final Map.Entry<String, Object> entry : sourceMap.entrySet()) {
                    if (excludeKeySet.contains(entry.getKey())) {
                        continue;
                    }
                    targetList.add(Map.of(
                            config.getKeyName(), entry.getKey(),
                            config.getValueName(), entry.getValue()
                    ));
                }

                if (config.getRemoveProcessedFields()) {
                    Map<String, Object> modifiedSourceMap = new HashMap<>();
                    for (final Map.Entry<String, Object> entry : sourceMap.entrySet()) {
                        if (excludeKeySet.contains(entry.getKey())) {
                            modifiedSourceMap.put(entry.getKey(), entry.getValue());
                        }
                    }
                    recordEvent.put(config.getSource(), modifiedSourceMap);
                }

                recordEvent.put(config.getTarget(), targetList);
            } catch (Exception e) {
                LOG.error("Fail to perform Map to List operation", e);
                //TODO: add tagging on failure
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
