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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@DataPrepperPlugin(name = "copy_values", pluginType = Processor.class, pluginConfigurationType = CopyValueProcessorConfig.class)
public class CopyValueProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private final CopyValueProcessorConfig config;
    private final List<CopyValueProcessorConfig.Entry> entries;
    private final ExpressionEvaluator expressionEvaluator;

    @DataPrepperPluginConstructor
    public CopyValueProcessor(final PluginMetrics pluginMetrics, final CopyValueProcessorConfig config, final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.config = config;
        this.entries = config.getEntries();
        this.expressionEvaluator = expressionEvaluator;
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for(final Record<Event> record : records) {
            final Event recordEvent = record.getData();
            if (config.getMode() == CopyValueProcessorConfig.Mode.NORMAL) {
                for (CopyValueProcessorConfig.Entry entry : entries) {
                    if (Objects.nonNull(entry.getCopyWhen()) && !expressionEvaluator.evaluateConditional(entry.getCopyWhen(), recordEvent)) {
                        continue;
                    }

                    if (entry.getFromKey().equals(entry.getToKey()) || !recordEvent.containsKey(entry.getFromKey())) {
                        continue;
                    }

                    if (!recordEvent.containsKey(entry.getToKey()) || entry.getOverwriteIfToKeyExists()) {
                        final Object source = recordEvent.get(entry.getFromKey(), Object.class);
                        recordEvent.put(entry.getToKey(), source);
                    }
                }
            } else if (config.getMode() == CopyValueProcessorConfig.Mode.LIST) {
                List<Map<String, Object>> sourceList = recordEvent.get(config.getSource(), List.class);
                List<Map<String, Object>> targetList = new ArrayList<>();

                Map<CopyValueProcessorConfig.Entry, Boolean> whenConditions = new HashMap<>();
                for (CopyValueProcessorConfig.Entry entry : entries) {
                    if (Objects.nonNull(entry.getCopyWhen()) && !expressionEvaluator.evaluateConditional(entry.getCopyWhen(), recordEvent)) {
                        whenConditions.put(entry, Boolean.FALSE);
                    } else {
                        whenConditions.put(entry, Boolean.TRUE);
                    }
                }
                for (Map<String, Object> sourceField : sourceList) {
                    final Map<String, Object> targetItem = new HashMap<>();
                    for (CopyValueProcessorConfig.Entry entry : entries) {
                        if (!whenConditions.get(entry) || !sourceField.containsKey(entry.getFromKey())) {
                            continue;
                        }
                        targetItem.put(entry.getToKey(), sourceField.get(entry.getFromKey()));
                    }
                    targetList.add(targetItem);
                }
                recordEvent.put(config.getTarget(), targetList);
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
