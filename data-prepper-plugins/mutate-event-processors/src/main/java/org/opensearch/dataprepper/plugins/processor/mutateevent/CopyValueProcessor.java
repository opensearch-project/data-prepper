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

import java.util.Collection;
import java.util.List;
import java.util.Objects;

@DataPrepperPlugin(name = "copy_values", pluginType = Processor.class, pluginConfigurationType = CopyValueProcessorConfig.class)
public class CopyValueProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private final List<CopyValueProcessorConfig.Entry> entries;

    private final ExpressionEvaluator<Boolean> expressionEvaluator;

    @DataPrepperPluginConstructor
    public CopyValueProcessor(final PluginMetrics pluginMetrics, final CopyValueProcessorConfig config, final ExpressionEvaluator<Boolean> expressionEvaluator) {
        super(pluginMetrics);
        this.entries = config.getEntries();
        this.expressionEvaluator = expressionEvaluator;
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for(final Record<Event> record : records) {
            final Event recordEvent = record.getData();
            for(CopyValueProcessorConfig.Entry entry : entries) {
                if (Objects.nonNull(entry.getCopyWhen()) && !expressionEvaluator.evaluate(entry.getCopyWhen(), recordEvent)) {
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
