/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */
package org.opensearch.dataprepper.plugins.processor.mutateevent;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.EVENT;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;
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

@DataPrepperPlugin(name = "wrap_entries", pluginType = Processor.class, pluginConfigurationType = WrapEntriesProcessorConfig.class)
public class WrapEntriesProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(WrapEntriesProcessor.class);

    private final WrapEntriesProcessorConfig config;
    private final ExpressionEvaluator expressionEvaluator;

    @DataPrepperPluginConstructor
    public WrapEntriesProcessor(final PluginMetrics pluginMetrics,
                               final WrapEntriesProcessorConfig config,
                               final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.config = config;
        this.expressionEvaluator = expressionEvaluator;
        config.validateExpressions(expressionEvaluator);
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for (final Record<Event> record : records) {
            final Event event = record.getData();

            try {
                if (config.getWrapEntriesWhen() != null
                        && !expressionEvaluator.evaluateConditional(config.getWrapEntriesWhen(), event)) {
                    continue;
                }

                processEvent(event);
            } catch (final Exception e) {
                LOG.atError()
                        .addMarker(EVENT)
                        .addMarker(NOISY)
                        .setMessage("Error processing event [{}]")
                        .addArgument(event)
                        .setCause(e)
                        .log();
                addTagsOnFailure(event);
            }
        }
        return records;
    }

    private void processEvent(final Event event) {
        final String source = config.getSource();

        if (!event.containsKey(source)) {
            LOG.debug(EVENT, "Source key [{}] does not exist in event [{}], skipping.", source, event);
            return;
        }

        final Object sourceValue = event.get(source, Object.class);

        if (!(sourceValue instanceof List)) {
            LOG.warn(EVENT, "Source key [{}] is not a list in event [{}], skipping.", source, event);
            addTagsOnFailure(event);
            return;
        }

        final List<?> sourceList = (List<?>) sourceValue;

        if (sourceList.isEmpty()) {
            return;
        }

        final List<Map<String, Object>> result = new ArrayList<>(sourceList.size());
        final String key = config.getKey();

        for (final Object element : sourceList) {
            if (config.getExcludeNullEmptyValues()) {
                if (element == null) {
                    continue;
                }
                if (element instanceof String && ((String) element).isEmpty()) {
                    continue;
                }
            }
            final Map<String, Object> entry = new HashMap<>();
            entry.put(key, element);
            result.add(entry);
        }


        final String effectiveTarget = config.getEffectiveTarget();

        if (config.getAppendIfTargetExists() && event.containsKey(effectiveTarget)) {
            final Object existingValue = event.get(effectiveTarget, Object.class);
            if (!(existingValue instanceof List)) {
                LOG.warn(EVENT, "Target key [{}] exists but is not a list in event [{}], skipping.",
                        effectiveTarget, event);
                addTagsOnFailure(event);
                return;
            }
            final List<Object> existingList = new ArrayList<>((List<?>) existingValue);
            existingList.addAll(result);
            event.put(effectiveTarget, existingList);
        } else {
            event.put(effectiveTarget, result);
        }
    }

    private void addTagsOnFailure(final Event event) {
        final List<String> tags = config.getTagsOnFailure();
        if (tags != null && !tags.isEmpty()) {
            event.getMetadata().addTags(tags);
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
