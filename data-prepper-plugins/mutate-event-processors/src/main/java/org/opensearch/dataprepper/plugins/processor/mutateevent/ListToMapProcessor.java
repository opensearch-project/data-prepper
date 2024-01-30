/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

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
import java.util.Objects;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.EVENT;

@DataPrepperPlugin(name = "list_to_map", pluginType = Processor.class, pluginConfigurationType = ListToMapProcessorConfig.class)
public class ListToMapProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(ListToMapProcessor.class);
    private final ListToMapProcessorConfig config;

    private final ExpressionEvaluator expressionEvaluator;

    @DataPrepperPluginConstructor
    public ListToMapProcessor(final PluginMetrics pluginMetrics, final ListToMapProcessorConfig config, final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.config = config;
        this.expressionEvaluator = expressionEvaluator;
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for (final Record<Event> record : records) {
            final Event recordEvent = record.getData();

            if (Objects.nonNull(config.getListToMapWhen()) && !expressionEvaluator.evaluateConditional(config.getListToMapWhen(), recordEvent)) {
                continue;
            }

            final List<Map<String, Object>> sourceList;
            try {
                sourceList = recordEvent.get(config.getSource(), List.class);
            } catch (final Exception e) {
                LOG.warn(EVENT, "Given source path [{}] is not valid on record [{}]",
                        config.getSource(), recordEvent, e);
                //TODO: Add tags on failure
                continue;
            }


            final Map<String, Object> targetMap;
            try {
                targetMap = constructTargetMap(sourceList);
            } catch (IllegalArgumentException e) {
                LOG.warn(EVENT, "Cannot find a list at the given source path [{}} on record [{}]",
                        config.getSource(), recordEvent, e);
                //TODO: Add tags on failure
                continue;
            } catch (final Exception e) {
                LOG.error(EVENT, "Error converting source list to map on record [{}]", recordEvent, e);
                //TODO: Add tags on failure
                continue;
            }

            try {
                updateEvent(recordEvent, targetMap);
            } catch (final Exception e) {
                LOG.error(EVENT, "Error updating record [{}] after converting source list to map", recordEvent, e);
                //TODO: Add tags on failure
            }
        }
        return records;
    }

    private Map<String, Object> constructTargetMap(final List<Map<String, Object>> sourceList) {
        Map<String, Object> targetMap = new HashMap<>();
        for (final Map<String, Object> item : sourceList) {
            final String itemKey = (String) item.get(config.getKey());
            if (!config.getFlatten()) {
                final List<Object> itemValue;
                if (!targetMap.containsKey(itemKey)) {
                    itemValue = new ArrayList<>();
                    targetMap.put((String)item.get(config.getKey()), itemValue);
                } else {
                    itemValue = (List<Object>) targetMap.get(itemKey);
                }

                if (config.getValueKey() == null) {
                    itemValue.add(item);
                } else {
                    itemValue.add(item.get(config.getValueKey()));
                }
            } else {
                if (!targetMap.containsKey(itemKey) || config.getFlattenedElement() == ListToMapProcessorConfig.FlattenedElement.LAST) {
                    if (config.getValueKey() == null) {
                        targetMap.put(itemKey, item);
                    } else  {
                        targetMap.put(itemKey, item.get(config.getValueKey()));
                    }
                }
            }
        }
        return targetMap;
    }

    private void updateEvent(Event recordEvent, Map<String, Object> targetMap) {
        final boolean doWriteToRoot = Objects.isNull(config.getTarget());
        if (doWriteToRoot) {
            for (final Map.Entry<String, Object> entry : targetMap.entrySet()) {
                recordEvent.put(entry.getKey(), entry.getValue());
            }
        } else {
            recordEvent.put(config.getTarget(), targetMap);
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
