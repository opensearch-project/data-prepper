/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.drop;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.annotations.DataPrepperPluginConstructor;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.processor.AbstractProcessor;
import com.amazon.dataprepper.model.processor.Processor;
import com.amazon.dataprepper.model.record.Record;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;

import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

@DataPrepperPlugin(name = "drop_events", pluginType = Processor.class, pluginConfigurationType = DropEventProcessorConfig.class)
public class DropEventsProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final String WHEN_PLUGIN_SETTING_KEY = "when";
    private static final String HANDLE_FAILED_EVENTS_KEY = "handle_failed_events";

    private final DropEventsWhenCondition whenCondition;

    @DataPrepperPluginConstructor
    public DropEventsProcessor(
            final PluginMetrics pluginMetrics,
            final DropEventProcessorConfig dropEventProcessorConfig,
            final ExpressionEvaluator<Boolean> expressionEvaluator
    ) {
        super(pluginMetrics);

        whenCondition = new DropEventsWhenCondition.Builder()
                .withDropEventsProcessorConfig(dropEventProcessorConfig)
                .withExpressionEvaluator(expressionEvaluator)
                .build();
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        if (whenCondition.shouldEvaluateConditional()) {
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
