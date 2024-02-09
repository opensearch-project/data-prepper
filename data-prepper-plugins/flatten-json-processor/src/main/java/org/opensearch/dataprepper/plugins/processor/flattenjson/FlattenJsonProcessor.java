/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.flattenjson;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.wnameless.json.flattener.JsonFlattener;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

@DataPrepperPlugin(name = "flatten", pluginType = Processor.class, pluginConfigurationType = FlattenJsonProcessorConfig.class)
public class FlattenJsonProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(FlattenJsonProcessor.class);
    private final FlattenJsonProcessorConfig config;
    private final ExpressionEvaluator expressionEvaluator;

    @DataPrepperPluginConstructor
    public FlattenJsonProcessor(final PluginMetrics pluginMetrics, final FlattenJsonProcessorConfig config, final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.config = config;
        this.expressionEvaluator = expressionEvaluator;
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for (final Record<Event> record : records) {
            final Event recordEvent = record.getData();

            if (config.getFlattenWhen() != null && !expressionEvaluator.evaluateConditional(config.getFlattenWhen(), recordEvent)) {
                continue;
            }

            final String sourceJson = recordEvent.getAsJsonString(config.getSource());

            Map<String, Object> flattenJson = new JsonFlattener(sourceJson).ignoreReservedCharacters().flattenAsMap();

            recordEvent.put(config.getTarget(), flattenJson);
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
