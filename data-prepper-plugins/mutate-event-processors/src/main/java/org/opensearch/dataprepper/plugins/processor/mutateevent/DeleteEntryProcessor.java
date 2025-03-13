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
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

@DataPrepperPlugin(name = "delete_entries", pluginType = Processor.class, pluginConfigurationType = DeleteEntryProcessorConfig.class)
public class DeleteEntryProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {

    private static final Logger LOG = LoggerFactory.getLogger(DeleteEntryProcessor.class);
    private final List<EventKey> withKeys;
    private final String deleteWhen;
    private final List<DeleteEntryProcessorConfig.Entry> entries;

    private final ExpressionEvaluator expressionEvaluator;

    @DataPrepperPluginConstructor
    public DeleteEntryProcessor(final PluginMetrics pluginMetrics, final DeleteEntryProcessorConfig config, final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.withKeys = config.getWithKeys();
        this.deleteWhen = config.getDeleteWhen();
        this.expressionEvaluator = expressionEvaluator;

        if (deleteWhen != null
                && !expressionEvaluator.isValidExpressionStatement(deleteWhen)) {
            throw new InvalidPluginConfigurationException(
                    String.format("delete_when %s is not a valid expression statement. See https://opensearch" +
                            ".org/docs/latest/data-prepper/pipelines/expression-syntax/ for valid expression syntax", deleteWhen));
        }

        if (this.withKeys != null && !this.withKeys.isEmpty()) {
            DeleteEntryProcessorConfig.Entry entry = new DeleteEntryProcessorConfig.Entry(this.withKeys, this.deleteWhen);
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
        });
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for(final Record<Event> record : records) {
            final Event recordEvent = record.getData();

            try {
                for (final DeleteEntryProcessorConfig.Entry entry : entries) {
                    if (Objects.nonNull(entry.getDeleteWhen()) && !expressionEvaluator.evaluateConditional(entry.getDeleteWhen(),
                            recordEvent)) {
                        continue;
                    }

                    for (final EventKey key : entry.getWithKeys()) {
                        recordEvent.delete(key);
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
