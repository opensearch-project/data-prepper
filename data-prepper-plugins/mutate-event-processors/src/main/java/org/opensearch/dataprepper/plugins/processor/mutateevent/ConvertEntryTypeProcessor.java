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
import org.opensearch.dataprepper.typeconverter.ConverterArguments;
import org.opensearch.dataprepper.typeconverter.TypeConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.EVENT;

@DataPrepperPlugin(name = "convert_entry_type", pluginType = Processor.class, pluginConfigurationType = ConvertEntryTypeProcessorConfig.class)
public class ConvertEntryTypeProcessor  extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(ConvertEntryTypeProcessor.class);
    private final List<String> convertEntryKeys;
    private final TypeConverter<?> converter;
    private final String convertWhen;
    private final List<String> nullValues;
    private final String type;
    private final List<String> tagsOnFailure;
    private int scale = 0;

    private final ExpressionEvaluator expressionEvaluator;
    private final ConverterArguments converterArguments;

    @DataPrepperPluginConstructor
    public ConvertEntryTypeProcessor(final PluginMetrics pluginMetrics,
                                     final ConvertEntryTypeProcessorConfig convertEntryTypeProcessorConfig,
                                     final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.converterArguments = convertEntryTypeProcessorConfig;
        this.convertEntryKeys = getKeysToConvert(convertEntryTypeProcessorConfig);
        TargetType targetType = convertEntryTypeProcessorConfig.getType();
        this.type = targetType.name();
        this.converter = targetType.getTargetConverter();
        this.scale = convertEntryTypeProcessorConfig.getScale();
        this.convertWhen = convertEntryTypeProcessorConfig.getConvertWhen();
        this.nullValues = convertEntryTypeProcessorConfig.getNullValues()
                .orElse(List.of());
        this.expressionEvaluator = expressionEvaluator;
        this.tagsOnFailure = convertEntryTypeProcessorConfig.getTagsOnFailure();
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for(final Record<Event> record : records) {
            final Event recordEvent = record.getData();

            try {

                if (Objects.nonNull(convertWhen) && !expressionEvaluator.evaluateConditional(convertWhen, recordEvent)) {
                    continue;
                }

                for (final String key : convertEntryKeys) {
                    Object keyVal = recordEvent.get(key, Object.class);
                    if (keyVal != null) {
                        if (!nullValues.contains(keyVal.toString())) {
                            try {
                                recordEvent.put(key, converter.convert(keyVal, converterArguments));
                            } catch (final RuntimeException e) {
                                LOG.error(EVENT, "Unable to convert key: {} with value: {} to {}", key, keyVal, type, e);
                                recordEvent.getMetadata().addTags(tagsOnFailure);
                            }
                        } else {
                            recordEvent.delete(key);
                        }
                    }
                }
            } catch (final Exception e) {
                LOG.error(EVENT, "There was an exception while processing Event [{}]", recordEvent, e);
                recordEvent.getMetadata().addTags(tagsOnFailure);
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

    private List<String> getKeysToConvert(final ConvertEntryTypeProcessorConfig convertEntryTypeProcessorConfig) {
        final String key = convertEntryTypeProcessorConfig.getKey();
        final List<String> keys = convertEntryTypeProcessorConfig.getKeys();
        if (key == null && keys == null) {
            throw new IllegalArgumentException("key and keys cannot both be null. One must be provided.");
        }
        if (key != null && keys != null) {
            throw new IllegalArgumentException("key and keys cannot both be defined.");
        }
        if (key != null) {
            if (key.isEmpty()) {
                throw new IllegalArgumentException("key cannot be empty.");
            } else {
                return Collections.singletonList(key);
            }
        }
        return keys;
    }
}


