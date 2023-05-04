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
import org.opensearch.dataprepper.typeconverter.TypeConverter;

import java.util.Collection;
import java.util.Objects;

@DataPrepperPlugin(name = "convert_entry_type", pluginType = Processor.class, pluginConfigurationType = ConvertEntryTypeProcessorConfig.class)
public class ConvertEntryTypeProcessor  extends AbstractProcessor<Record<Event>, Record<Event>> {
    private final String key;
    private final TypeConverter converter;
    private final String convertWhen;

    private final ExpressionEvaluator<Boolean> expressionEvaluator;

    @DataPrepperPluginConstructor
    public ConvertEntryTypeProcessor(final PluginMetrics pluginMetrics,
                                     final ConvertEntryTypeProcessorConfig convertEntryTypeProcessorConfig,
                                     final ExpressionEvaluator<Boolean> expressionEvaluator) {
        super(pluginMetrics);
        this.key = convertEntryTypeProcessorConfig.getKey();
        this.converter = convertEntryTypeProcessorConfig.getType().getTargetConverter();
        this.convertWhen = convertEntryTypeProcessorConfig.getConvertWhen();
        this.expressionEvaluator = expressionEvaluator;
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for(final Record<Event> record : records) {
            final Event recordEvent = record.getData();

            if (Objects.nonNull(convertWhen) && !expressionEvaluator.evaluate(convertWhen, recordEvent)) {
                continue;
            }

            Object keyVal = recordEvent.get(key, Object.class);
            if (keyVal != null) {
                recordEvent.delete(key);
                recordEvent.put(key, this.converter.convert(keyVal));
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


