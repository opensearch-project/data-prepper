/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.drop;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.annotations.SingleThread;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;

import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

@SingleThread
@DataPrepperPlugin(name = "drop_events", pluginType = Processor.class, pluginConfigurationType = DropEventProcessorConfig.class)
public class DropEventsProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {

    private final DropEventsWhenCondition whenCondition;

    @DataPrepperPluginConstructor
    public DropEventsProcessor(
            final PluginMetrics pluginMetrics,
            final DropEventProcessorConfig dropEventProcessorConfig,
            final ExpressionEvaluator expressionEvaluator
    ) {
        super(pluginMetrics);

        if (dropEventProcessorConfig.getDropWhen() != null &&
                (!expressionEvaluator.isValidExpressionStatement(dropEventProcessorConfig.getDropWhen()))) {
            throw new InvalidPluginConfigurationException("drop_when {} is not a valid expression statement. See https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/ for valid expression syntax");
        }

        whenCondition = new DropEventsWhenCondition.Builder()
                .withDropEventsProcessorConfig(dropEventProcessorConfig)
                .withExpressionEvaluator(expressionEvaluator)
                .build();
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        if (whenCondition.isNotAlwaysTrue()) {
            return records.stream()
                    .filter(record -> whenCondition.isStatementFalseWith(record.getData()))
                    .collect(Collectors.toList());
        }
        else {
            return Collections.emptyList();
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
