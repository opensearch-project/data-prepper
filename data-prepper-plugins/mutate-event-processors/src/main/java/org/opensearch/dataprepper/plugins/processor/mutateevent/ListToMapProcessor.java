/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.EVENT;

@DataPrepperPlugin(name = "list_to_map", pluginType = Processor.class, pluginConfigurationType = ListToMapProcessorConfig.class)
public class ListToMapProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(ListToMapProcessor.class);
    private final ListToMapProcessorConfig config;

    private final ExpressionEvaluator<Boolean> expressionEvaluator;

    @DataPrepperPluginConstructor
    public ListToMapProcessor(final PluginMetrics pluginMetrics, final ListToMapProcessorConfig config, final ExpressionEvaluator<Boolean> expressionEvaluator) {
        super(pluginMetrics);
        this.config = config;
        this.expressionEvaluator = expressionEvaluator;
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for (final Record<Event> record : records) {
            final Event recordEvent = record.getData();

            if (Objects.nonNull(config.getListToMapWhen()) && !expressionEvaluator.evaluate(config.getListToMapWhen(), recordEvent)) {
                continue;
            }

            final JsonNode sourceNode;
            try {
                sourceNode = getSourceNode(recordEvent);
            } catch (final Exception e) {
                LOG.warn(EVENT, "Given source path [{}] is not valid on record [{}]",
                        config.getSource(), recordEvent, e);
                continue;
            }

            ObjectNode targetNode;
            try {
                targetNode = constructTargetNode(sourceNode);
            } catch (IllegalArgumentException e) {
                LOG.warn(EVENT, "Cannot find a list at the given source path [{}} on record [{}]",
                        config.getSource(), recordEvent, e);
                continue;
            } catch (final Exception e) {
                LOG.error(EVENT, "Error converting source list to map on record [{}]", recordEvent, e);
                continue;
            }

            try {
                updateEvent(recordEvent, targetNode);
            } catch (final Exception e) {
                LOG.error(EVENT, "Error updating record [{}] after converting source list to map", recordEvent, e);
            }
        }
        return records;
    }

    private JsonNode getSourceNode(final Event recordEvent) {
        final Object sourceObject = recordEvent.get(config.getSource(), Object.class);
        return OBJECT_MAPPER.convertValue(sourceObject, JsonNode.class);
    }

    private ObjectNode constructTargetNode(JsonNode sourceNode) {
        final ObjectNode targetNode = OBJECT_MAPPER.createObjectNode();
        if (sourceNode.isArray()) {
            for (final JsonNode itemNode : sourceNode) {
                String itemKey = itemNode.get(config.getKey()).asText();

                if (!config.getFlatten()) {
                    final ArrayNode itemValueNode;
                    if (!targetNode.has(itemKey)) {
                        itemValueNode = OBJECT_MAPPER.createArrayNode();
                        targetNode.set(itemKey, itemValueNode);
                    } else {
                        itemValueNode = (ArrayNode) targetNode.get(itemKey);
                    }

                    if (config.getValueKey() == null) {
                        itemValueNode.add(itemNode);
                    } else {
                        itemValueNode.add(itemNode.get(config.getValueKey()));
                    }
                } else {
                    if (!targetNode.has(itemKey) || config.getFlattenedElement() == ListToMapProcessorConfig.FlattenedElement.LAST) {
                        if (config.getValueKey() == null) {
                            targetNode.set(itemKey, itemNode);
                        } else  {
                            targetNode.set(itemKey, itemNode.get(config.getValueKey()));
                        }
                    }
                }
            }
        } else {
            throw new IllegalArgumentException("Cannot find a list at the given source path [{}]" + config.getSource());
        }
        return targetNode;
    }

    private void updateEvent(Event recordEvent, ObjectNode targetNode) {
        final TypeReference<Map<String, Object>> mapTypeReference = new TypeReference<>() {};
        final Map<String, Object> targetMap = OBJECT_MAPPER.convertValue(targetNode, mapTypeReference);

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
