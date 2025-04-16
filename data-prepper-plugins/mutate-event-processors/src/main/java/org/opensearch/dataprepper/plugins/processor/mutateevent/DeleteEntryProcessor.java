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
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@DataPrepperPlugin(name = "delete_entries", pluginType = Processor.class, pluginConfigurationType = DeleteEntryProcessorConfig.class)
public class DeleteEntryProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(DeleteEntryProcessor.class);
    private final List<EventKey> entries;
    private final String deleteWhen;

    private final ExpressionEvaluator expressionEvaluator;
    private final DeleteEntryProcessorConfig deleteEntryProcessorConfig;

    @DataPrepperPluginConstructor
    public DeleteEntryProcessor(final PluginMetrics pluginMetrics, final DeleteEntryProcessorConfig config, final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.deleteEntryProcessorConfig = config;
        this.entries = config.getWithKeys();
        this.deleteWhen = config.getDeleteWhen();
        this.expressionEvaluator = expressionEvaluator;

        if (deleteWhen != null
                    && !expressionEvaluator.isValidExpressionStatement(deleteWhen)) {
                throw new InvalidPluginConfigurationException(
                        String.format("delete_when %s is not a valid expression statement. See https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/ for valid expression syntax", deleteWhen));
        }
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for(final Record<Event> record : records) {
            final Event recordEvent = record.getData();

            try {
                final String iterateOn = deleteEntryProcessorConfig.getIterateOn();
                if (Objects.isNull(iterateOn)) {
                    if (Objects.nonNull(deleteWhen) && !expressionEvaluator.evaluateConditional(deleteWhen, recordEvent)) {
                        continue;
                    }


                    for (final EventKey entry : entries) {
                        recordEvent.delete(entry);
                    }
                } else {
                    final boolean applyEventDeleteWhen = !deleteEntryProcessorConfig.isUseIterateOnContext() &&
                            Objects.nonNull(deleteWhen);
                    final boolean applyIterateDeleteWhen = deleteEntryProcessorConfig.isUseIterateOnContext() &&
                            Objects.nonNull(deleteWhen);
                    if (applyEventDeleteWhen && !expressionEvaluator.evaluateConditional(deleteWhen, recordEvent)) {
                        continue;
                    }
                    final List<Map<String, Object>> iterateOnList = recordEvent.get(iterateOn, List.class);
                    if (iterateOnList != null) {
                        final List<Map<String, Object>> result = new ArrayList<>();
                        for (final Map<String, Object> item : iterateOnList) {
                            final Event context = JacksonEvent.builder()
                                    .withEventMetadata(recordEvent.getMetadata())
                                    .withData(item)
                                    .build();
                            if (applyIterateDeleteWhen &&
                                    !expressionEvaluator.evaluateConditional(deleteWhen, context)) {
                                result.add(item);
                                continue;
                            }

                            for (final EventKey entry : entries) {
                                context.delete(entry);
                            }
                            result.add(context.toMap());
                        }
                        recordEvent.put(iterateOn, result);
                    }
                }
            } catch (final Exception e) {
                LOG.atError()
                        .addMarker(EVENT)
                        .addMarker(NOISY)
                        .setMessage("There was an exception while processing Event [{}]")
                        .addArgument(recordEvent)
                        .setCause(e)
                        .log();
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
