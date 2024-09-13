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

@DataPrepperPlugin(name = "copy_values", pluginType = Processor.class, pluginConfigurationType = CopyValueProcessorConfig.class)
public class CopyValueProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(CopyValueProcessor.class);
    private final CopyValueProcessorConfig config;
    private final List<CopyValueProcessorConfig.Entry> entries;
    private final ExpressionEvaluator expressionEvaluator;

    @DataPrepperPluginConstructor
    public CopyValueProcessor(final PluginMetrics pluginMetrics, final CopyValueProcessorConfig config, final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.config = config;
        this.entries = config.getEntries();
        this.expressionEvaluator = expressionEvaluator;

        config.getEntries().forEach(entry -> {
            if (entry.getCopyWhen() != null
                    && !expressionEvaluator.isValidExpressionStatement(entry.getCopyWhen())) {
                throw new InvalidPluginConfigurationException(
                        String.format("copy_when %s is not a valid expression statement. See https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/ for valid expression syntax", entry.getCopyWhen()));
            }
        });
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for(final Record<Event> record : records) {
            final Event recordEvent = record.getData();

            try {
                if (config.getFromList() != null || config.getToList() != null) {
                    // Copying entries between lists
                    if (recordEvent.containsKey(config.getToList()) && !config.getOverwriteIfToListExists()) {
                        continue;
                    }

                    final List<Map<String, Object>> sourceList = recordEvent.get(config.getFromList(), List.class);
                    final List<Map<String, Object>> targetList = new ArrayList<>();

                    final Map<CopyValueProcessorConfig.Entry, Boolean> whenConditions = new HashMap<>();
                    for (final CopyValueProcessorConfig.Entry entry : entries) {
                        if (Objects.nonNull(entry.getCopyWhen()) && !expressionEvaluator.evaluateConditional(entry.getCopyWhen(), recordEvent)) {
                            whenConditions.put(entry, Boolean.FALSE);
                        } else {
                            whenConditions.put(entry, Boolean.TRUE);
                        }
                    }
                    for (final Map<String, Object> sourceField : sourceList) {
                        final Map<String, Object> targetItem = new HashMap<>();
                        for (final CopyValueProcessorConfig.Entry entry : entries) {
                            if (!whenConditions.get(entry) || !sourceField.containsKey(entry.getFromKey())) {
                                continue;
                            }
                            targetItem.put(entry.getToKey(), sourceField.get(entry.getFromKey()));
                        }
                        targetList.add(targetItem);
                    }
                    recordEvent.put(config.getToList(), targetList);
                } else {
                    // Copying individual entries
                    for (final CopyValueProcessorConfig.Entry entry : entries) {
                        if (shouldCopyEntry(entry, recordEvent)) {
                            final Object source = recordEvent.get(entry.getFromKey(), Object.class);
                            recordEvent.put(entry.getToKey(), source);
                        }
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

    private boolean shouldCopyEntry(final CopyValueProcessorConfig.Entry entry, final Event recordEvent) {
        if (Objects.nonNull(entry.getCopyWhen()) && !expressionEvaluator.evaluateConditional(entry.getCopyWhen(), recordEvent)) {
            return false;
        }

        if (entry.getFromKey().equals(entry.getToKey()) || !recordEvent.containsKey(entry.getFromKey())) {
            return false;
        }

        return !recordEvent.containsKey(entry.getToKey()) || entry.getOverwriteIfToKeyExists();
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
