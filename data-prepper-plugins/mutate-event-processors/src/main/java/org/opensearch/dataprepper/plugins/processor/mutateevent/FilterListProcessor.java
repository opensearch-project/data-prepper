/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.EVENT;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.SENSITIVE;

@DataPrepperPlugin(name = "filter_list", pluginType = Processor.class, pluginConfigurationType = FilterListProcessorConfig.class)
public class FilterListProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(FilterListProcessor.class);
    private static final String FAILED_ELEMENTS_METADATA_KEY = "filter_list_processor_failed_elements";
    private static final String FAILED_ELEMENTS_COUNT_METADATA_KEY = "filter_list_processor_failed_elements_count";
    private final FilterListProcessorConfig config;
    private final ExpressionEvaluator expressionEvaluator;

    @DataPrepperPluginConstructor
    public FilterListProcessor(final PluginMetrics pluginMetrics, final FilterListProcessorConfig config, final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.config = config;
        this.expressionEvaluator = expressionEvaluator;

        config.validateExpressions(expressionEvaluator);
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for (final Record<Event> record : records) {
            final Event recordEvent = record.getData();

            try {
                if (Objects.nonNull(config.getFilterListWhen()) && !expressionEvaluator.evaluateConditional(config.getFilterListWhen(), recordEvent)) {
                    continue;
                }

                final List<Object> sourceList;
                try {
                    sourceList = recordEvent.get(config.getIterateOn(), List.class);
                } catch (final Exception e) {
                    LOG.atWarn()
                            .addMarker(EVENT)
                            .addMarker(SENSITIVE)
                            .setMessage("Given source path [{}] is not valid on record [{}]")
                            .addArgument(config.getIterateOn())
                            .addArgument(recordEvent)
                            .setCause(e)
                            .log();
                    addTagsOnFailure(recordEvent);
                    continue;
                }

                if (sourceList == null) {
                    LOG.debug("Source list at path [{}] is null, skipping event", config.getIterateOn());
                    continue;
                }

                final List<Object> filteredList = new ArrayList<>();
                final List<Object> failedElements = new ArrayList<>();
                int failedElementCount = 0;

                for (final Object element : sourceList) {
                    @SuppressWarnings("unchecked")
                    final Map<String, Object> contextMap = element instanceof Map
                            ? (Map<String, Object>) element
                            : Collections.singletonMap("value", element);

                    try {
                        // TODO(#6609): Revisit this per-element Event construction when ExpressionEvaluator/JsonPointer
                        // internals support a lighter evaluation path that avoids full tree conversion.
                        final Event elementEvent = JacksonEvent.builder()
                                .withEventType("event")
                                .withData(contextMap)
                                .build();

                        if (expressionEvaluator.evaluateConditional(config.getKeepElementWhen(), elementEvent)) {
                            filteredList.add(element);
                        }
                    } catch (final Exception e) {
                        failedElementCount++;
                        failedElements.add(element);
                        LOG.atWarn()
                                .addMarker(EVENT)
                                .addMarker(SENSITIVE)
                                .setMessage("Error evaluating keep_element_when expression [{}] for element in source list at path [{}]")
                                .addArgument(config.getKeepElementWhen())
                                .addArgument(config.getIterateOn())
                                .setCause(e)
                                .log();
                    }
                }

                if (failedElementCount > 0) {
                    addTagsOnFailure(recordEvent);
                    recordEvent.getMetadata().setAttribute(FAILED_ELEMENTS_COUNT_METADATA_KEY, failedElementCount);
                    recordEvent.getMetadata().setAttribute(FAILED_ELEMENTS_METADATA_KEY, failedElements);
                }

                recordEvent.put(config.getTarget(), filteredList);

            } catch (final Exception e) {
                LOG.atError()
                        .addMarker(EVENT)
                        .addMarker(NOISY)
                        .setMessage("There was an exception while processing Event [{}]")
                        .addArgument(recordEvent)
                        .setCause(e)
                        .log();
                addTagsOnFailure(recordEvent);
            }
        }
        return records;
    }

    private void addTagsOnFailure(final Event event) {
        if (config.getTagsOnFailure() != null) {
            event.getMetadata().addTags(config.getTagsOnFailure());
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
