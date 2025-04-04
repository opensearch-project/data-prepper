/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@DataPrepperPlugin(name = "list_to_map", pluginType = Processor.class, pluginConfigurationType = ListToMapProcessorConfig.class)
public class ListToMapProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(ListToMapProcessor.class);
    private final ListToMapProcessorConfig config;

    private final ExpressionEvaluator expressionEvaluator;
    private final List<ListToMapProcessorConfig.Entry> entries;

    @DataPrepperPluginConstructor
    public ListToMapProcessor(final PluginMetrics pluginMetrics, final ListToMapProcessorConfig config, final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.config = config;
        this.expressionEvaluator = expressionEvaluator;

        if (config.getListToMapWhen() != null
                && !expressionEvaluator.isValidExpressionStatement(config.getListToMapWhen())) {
            throw new InvalidPluginConfigurationException(
                    String.format("list_to_map_when %s is not a valid expression statement. See https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/ for valid expression syntax",
                            config.getListToMapWhen()));
        }

        if (config.getSource() != null || config.getListToMapWhen() != null) {
            ListToMapProcessorConfig.Entry entry = new ListToMapProcessorConfig.Entry(
                    config.getSource(),
                    config.getTarget(),
                    config.getKey(),
                    config.getValueKey(),
                    config.getUseSourceKey(),
                    config.getExtractValue(),
                    config.getFlatten(),
                    config.getFlattenedElement(),
                    config.getTagsOnFailure(),
                    config.getListToMapWhen()
            );
            entries = List.of(entry);
        }
        else{
            entries = config.getEntries();
        }

        for (ListToMapProcessorConfig.Entry entry : entries) {
            if (entry.getListToMapWhen() != null && !expressionEvaluator.isValidExpressionStatement(entry.getListToMapWhen())) {
                throw new InvalidPluginConfigurationException(
                        String.format("list_to_map_when %s is not a valid expression statement. See https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/ for valid expression syntax",
                                entry.getListToMapWhen()));
            }
        }
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for (final Record<Event> record : records) {
            final Event recordEvent = record.getData();
            for (ListToMapProcessorConfig.Entry entry : entries) {
                try {

                    if (Objects.nonNull(entry.getListToMapWhen()) && !expressionEvaluator.evaluateConditional(entry.getListToMapWhen(), recordEvent)) {
                        continue;
                    }

                    final List<Map<String, Object>> sourceList;
                    try {
                        sourceList = recordEvent.get(entry.getSource(), List.class);
                    } catch (final Exception e) {
                        LOG.warn(EVENT, "Given source path [{}] is not valid on record [{}]",
                                entry.getSource(), recordEvent, e);
                        recordEvent.getMetadata().addTags(entry.getTagsOnFailure());
                        continue;
                    }

                    final Map<String, Object> targetMap;
                    try {
                        targetMap = constructTargetMap(sourceList, entry);
                    } catch (final IllegalArgumentException e) {
                        LOG.warn(EVENT, "Cannot find a list at the given source path [{}} on record [{}]",
                                entry.getSource(), recordEvent, e);
                        recordEvent.getMetadata().addTags(entry.getTagsOnFailure());
                        continue;
                    } catch (final Exception e) {
                        LOG.atError()
                                .addMarker(EVENT)
                                .addMarker(NOISY)
                                .setMessage("Error converting source list to map on record [{}]")
                                .addArgument(recordEvent)
                                .setCause(e)
                                .log();
                        recordEvent.getMetadata().addTags(entry.getTagsOnFailure());
                        continue;
                    }

                    try {
                        updateEvent(recordEvent, targetMap, entry.getTarget());
                    } catch (final Exception e) {
                        LOG.atError()
                                .addMarker(EVENT)
                                .addMarker(NOISY)
                                .setMessage("Error updating record [{}] after converting source list to map")
                                .addArgument(recordEvent)
                                .setCause(e)
                                .log();
                        recordEvent.getMetadata().addTags(entry.getTagsOnFailure());
                    }
                } catch (final Exception e) {
                    LOG.atError()
                            .addMarker(EVENT)
                            .addMarker(NOISY)
                            .setMessage("There was an exception while processing Event [{}]")
                            .addArgument(recordEvent)
                            .setCause(e)
                            .log();
                    recordEvent.getMetadata().addTags(entry.getTagsOnFailure());
                }
            }
        }
        return records;
    }

    private Map<String, Object> constructTargetMap(final List<Map<String, Object>> sourceList, ListToMapProcessorConfig.Entry entry) {
        Map<String, Object> targetMap = new HashMap<>();
        for (final Map<String, Object> itemMap : sourceList) {

            if (entry.getUseSourceKey()) {
                if (entry.getFlatten()) {
                    for (final String entryKey : itemMap.keySet()) {
                        setTargetMapFlattened(targetMap, itemMap, entryKey, entryKey, entry.getExtractValue(), entry.getFlattenedElement());
                    }
                } else {
                    for (final String entryKey : itemMap.keySet()) {
                        setTargetMapUnflattened(targetMap, itemMap, entryKey, entryKey, entry.getExtractValue());
                    }
                }
            } else {
                final String itemKey = (String) itemMap.get(entry.getKey());
                if (entry.getFlatten()) {
                    setTargetMapFlattened(targetMap, itemMap, itemKey, entry.getValueKey(), entry.getValueKey() != null, entry.getFlattenedElement());
                } else {
                    setTargetMapUnflattened(targetMap, itemMap, itemKey, entry.getValueKey(), entry.getValueKey() != null);
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
            final Map<String, Object> targetMap, final Map<String, Object> itemMap, final String itemKey, final String itemValueKey,
            final boolean doExtractValue, final ListToMapProcessorConfig.FlattenedElement flattenedElement) {
        if (!targetMap.containsKey(itemKey) || flattenedElement == ListToMapProcessorConfig.FlattenedElement.LAST) {
            if (doExtractValue) {
                targetMap.put(itemKey, itemMap.get(itemValueKey));
            } else {
                targetMap.put(itemKey, itemMap);
            }
        }
    }

    private void updateEvent(final Event recordEvent, final Map<String, Object> targetMap, final String target) {
        final boolean doWriteToRoot = Objects.isNull(target);
        if (doWriteToRoot) {
            for (final Map.Entry<String, Object> entry : targetMap.entrySet()) {
                recordEvent.put(entry.getKey(), entry.getValue());
            }
        } else {
            recordEvent.put(target, targetMap);
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
