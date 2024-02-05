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
                recordEvent.getMetadata().addTags(config.getTagsOnFailure());
                continue;
            }

            final Map<String, Object> targetMap;
            try {
                targetMap = constructTargetMap(sourceList);
            } catch (final IllegalArgumentException e) {
                LOG.warn(EVENT, "Cannot find a list at the given source path [{}} on record [{}]",
                        config.getSource(), recordEvent, e);
                recordEvent.getMetadata().addTags(config.getTagsOnFailure());
                continue;
            } catch (final Exception e) {
                LOG.error(EVENT, "Error converting source list to map on record [{}]", recordEvent, e);
                recordEvent.getMetadata().addTags(config.getTagsOnFailure());
                continue;
            }

            try {
                updateEvent(recordEvent, targetMap);
            } catch (final Exception e) {
                LOG.error(EVENT, "Error updating record [{}] after converting source list to map", recordEvent, e);
                recordEvent.getMetadata().addTags(config.getTagsOnFailure());
            }
        }
        return records;
    }

    private Map<String, Object> constructTargetMap(final List<Map<String, Object>> sourceList) {
        Map<String, Object> targetMap = new HashMap<>();
        for (final Map<String, Object> itemMap : sourceList) {

            if (config.getUseSourceKey()) {
                if (config.getFlatten()) {
                    for (final String entryKey : itemMap.keySet()) {
                        setTargetMapFlattened(targetMap, itemMap, entryKey, entryKey, config.getExtractValue());
                    }
                } else {
                    for (final String entryKey : itemMap.keySet()) {
                        setTargetMapUnflattened(targetMap, itemMap, entryKey, entryKey, config.getExtractValue());
                    }
                }
            } else {
                final String itemKey = (String) itemMap.get(config.getKey());
                if (config.getFlatten()) {
                    setTargetMapFlattened(targetMap, itemMap, itemKey, config.getValueKey(), config.getValueKey() != null);
                } else {
                    setTargetMapUnflattened(targetMap, itemMap, itemKey, config.getValueKey(), config.getValueKey() != null);
                }
            }
        }
        return targetMap;
    }

    private void setTargetMapUnflattened(
            final Map<String, Object> targetMap, final Map<String, Object> itemMap, final String itemKey, final String itemValueKey, final boolean doExtractValue) {
        if (!targetMap.containsKey(itemKey)) {
            targetMap.put(itemKey, new ArrayList<>());
        }

        final List<Object> itemValue = (List<Object>) targetMap.get(itemKey);

        if (doExtractValue) {
            itemValue.add(itemMap.get(itemValueKey));
        } else {
            itemValue.add(itemMap);
        }
    }

    private void setTargetMapFlattened(
            final Map<String, Object> targetMap, final Map<String, Object> itemMap, final String itemKey, final String itemValueKey, final boolean doExtractValue) {
        if (!targetMap.containsKey(itemKey) || config.getFlattenedElement() == ListToMapProcessorConfig.FlattenedElement.LAST) {
            if (doExtractValue) {
                targetMap.put(itemKey, itemMap.get(itemValueKey));
            } else {
                targetMap.put(itemKey, itemMap);
            }
        }
    }

    private void updateEvent(final Event recordEvent, final Map<String, Object> targetMap) {
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
