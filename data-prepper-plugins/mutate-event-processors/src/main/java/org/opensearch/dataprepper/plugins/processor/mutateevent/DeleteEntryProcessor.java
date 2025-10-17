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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@DataPrepperPlugin(name = "delete_entries", pluginType = Processor.class, pluginConfigurationType = DeleteEntryProcessorConfig.class)
public class DeleteEntryProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(DeleteEntryProcessor.class);
    private final List<EventKey> withKeys;
    private final String deleteWhen;
    private final List<DeleteEntryProcessorConfig.Entry> entries;

    private final ExpressionEvaluator expressionEvaluator;
    private final DeleteEntryProcessorConfig deleteEntryProcessorConfig;

    @DataPrepperPluginConstructor
    public DeleteEntryProcessor(final PluginMetrics pluginMetrics, final DeleteEntryProcessorConfig config, final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.withKeys = config.getWithKeys();
        this.deleteEntryProcessorConfig = config;
        this.deleteWhen = config.getDeleteWhen();
        this.expressionEvaluator = expressionEvaluator;

        if (deleteWhen != null
                && !expressionEvaluator.isValidExpressionStatement(deleteWhen)) {
            throw new InvalidPluginConfigurationException(
                    String.format("delete_when %s is not a valid expression statement. See https://opensearch" +
                            ".org/docs/latest/data-prepper/pipelines/expression-syntax/ for valid expression syntax", deleteWhen));
        }

        if (this.withKeys != null && !this.withKeys.isEmpty()) {
            DeleteEntryProcessorConfig.Entry entry = new DeleteEntryProcessorConfig.Entry(this.withKeys, this.deleteWhen, config.getIterateOn(), config.getDeleteFromElementWhen());
            this.entries = List.of(entry);
        } else {
            this.entries = config.getEntries();
        }

        this.entries.forEach(entry -> {
            if (entry.getDeleteWhen() != null
                    && !expressionEvaluator.isValidExpressionStatement(entry.getDeleteWhen())) {
                throw new InvalidPluginConfigurationException(
                        String.format("delete_when %s is not a valid expression statement. See https://opensearch" +
                                        ".org/docs/latest/data-prepper/pipelines/expression-syntax/ for valid expression syntax",
                                entry.getDeleteWhen()));
            }

            if (entry.getIterateOn() == null && entry.getDeleteFromElementWhen() != null) {
                throw new InvalidPluginConfigurationException("delete_from_element_when only applies when iterate_on is configured.");
            }

            if (entry.getDeleteFromElementWhen() != null
                    && !expressionEvaluator.isValidExpressionStatement(entry.getDeleteFromElementWhen())) {
                throw new InvalidPluginConfigurationException(
                        String.format("delete_from_element_when %s is not a valid expression statement. See https://opensearch" +
                                        ".org/docs/latest/data-prepper/pipelines/expression-syntax/ for valid expression syntax",
                                entry.getDeleteFromElementWhen()));
            }
        });
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for (final Record<Event> record : records) {
            final Event recordEvent = record.getData();
            try {
                for (final DeleteEntryProcessorConfig.Entry entry : entries) {
                    if (Objects.nonNull(entry.getDeleteWhen()) && !expressionEvaluator.evaluateConditional(entry.getDeleteWhen(), recordEvent)) {
                        continue;
                    }

                    final String iterateOn = deleteEntryProcessorConfig.getIterateOn();
                    if (Objects.isNull(iterateOn)) {

                        for (final EventKey entryKey : entry.getWithKeys()) {
                            recordEvent.delete(entryKey);
                        }
                    } else {
                        handleForIterateOn(recordEvent, entry, iterateOn);
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

    private void handleForIterateOn(final Event recordEvent,
                                    final DeleteEntryProcessorConfig.Entry entry,
                                    final String iterateOn) {
        final List<Map<String, Object>> iterateOnList = recordEvent.get(iterateOn, List.class);
        if (iterateOnList != null) {
            for (int i = 0; i < iterateOnList.size(); i++) {
                final Map<String, Object> item = iterateOnList.get(i);
                final Event context = JacksonEvent.builder()
                        .withEventMetadata(recordEvent.getMetadata())
                        .withData(item)
                        .build();
                if (entry.getDeleteFromElementWhen() != null &&
                        !expressionEvaluator.evaluateConditional(entry.getDeleteFromElementWhen(), context)) {
                    continue;
                }

                for (final EventKey entryKey : entry.getWithKeys()) {
                    context.delete(entryKey);
                }
                iterateOnList.set(i, context.toMap());
            }
            recordEvent.put(iterateOn, iterateOnList);
        }
    }
}
