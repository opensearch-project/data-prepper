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
import org.opensearch.dataprepper.model.event.exceptions.EventKeyNotFoundException;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

@DataPrepperPlugin(name = "add_entries", pluginType = Processor.class, pluginConfigurationType = AddEntryProcessorConfig.class)
public class AddEntryProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(AddEntryProcessor.class);
    private final List<AddEntryProcessorConfig.Entry> entries;

    private final ExpressionEvaluator expressionEvaluator;

    @DataPrepperPluginConstructor
    public AddEntryProcessor(final PluginMetrics pluginMetrics, final AddEntryProcessorConfig config, final ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);
        this.entries = config.getEntries();
        this.expressionEvaluator = expressionEvaluator;

        config.getEntries().forEach(entry -> {
            if (entry.getAddWhen() != null
                    && !expressionEvaluator.isValidExpressionStatement(entry.getAddWhen())) {
                throw new InvalidPluginConfigurationException(
                        String.format("add_when %s is not a valid expression statement. See https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/ for valid expression syntax", entry.getAddWhen()));
            }
        });
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for(final Record<Event> record : records) {
            final Event recordEvent = record.getData();

            try {
                for (AddEntryProcessorConfig.Entry entry : entries) {

                    if (Objects.nonNull(entry.getAddWhen()) && !expressionEvaluator.evaluateConditional(entry.getAddWhen(), recordEvent)) {
                        continue;
                    }

                    try {
                        final String key = (entry.getKey() == null) ? null : recordEvent.formatString(entry.getKey(), expressionEvaluator);
                        final String metadataKey = entry.getMetadataKey();
                        Object value;
                        if (!Objects.isNull(entry.getValueExpression())) {
                            value = expressionEvaluator.evaluate(entry.getValueExpression(), recordEvent);
                        } else if (!Objects.isNull(entry.getFormat())) {
                            try {
                                value = recordEvent.formatString(entry.getFormat());
                            } catch (final EventKeyNotFoundException e) {
                                value = null;
                            }
                        } else {
                            value = entry.getValue();
                        }
                        if (!Objects.isNull(key)) {
                            if (!recordEvent.containsKey(key) || entry.getOverwriteIfKeyExists()) {
                                recordEvent.put(key, value);
                            } else if (recordEvent.containsKey(key) && entry.getAppendIfKeyExists()) {
                                mergeValueToEvent(recordEvent, key, value);
                            }
                        } else {
                            Map<String, Object> attributes = recordEvent.getMetadata().getAttributes();
                            if (!attributes.containsKey(metadataKey) || entry.getOverwriteIfKeyExists()) {
                                recordEvent.getMetadata().setAttribute(metadataKey, value);
                            } else if (attributes.containsKey(metadataKey) && entry.getAppendIfKeyExists()) {
                                mergeValueToEventMetadata(recordEvent, metadataKey, value);
                            }
                        }
                    } catch (Exception e) {
                        LOG.atError()
                                .addMarker(EVENT)
                                .addMarker(NOISY)
                                .setMessage("Error adding entry to record [{}] with key [{}], metadataKey [{}], value_expression [{}] format [{}], value [{}]")
                                .addArgument(recordEvent)
                                .addArgument(entry.getKey())
                                .addArgument(entry.getMetadataKey())
                                .addArgument(entry.getValueExpression())
                                .addArgument(entry.getFormat())
                                .addArgument(entry.getValue())
                                .setCause(e)
                                .log();
                    }
                }
            } catch(final Exception e){
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

    private void mergeValueToEvent(final Event recordEvent, final String key, final Object value) {
        mergeValue(value, () -> recordEvent.get(key, Object.class), newValue -> recordEvent.put(key, newValue));
    }

    private void mergeValueToEventMetadata(final Event recordEvent, final String key, final Object value) {
        mergeValue(value, () -> recordEvent.getMetadata().getAttribute(key), newValue -> recordEvent.getMetadata().setAttribute(key, newValue));
    }

    private void mergeValue(final Object value, Supplier<Object> getter, Consumer<Object> setter) {
        final Object currentValue = getter.get();
        final List<Object> mergedValue = new ArrayList<>();
        if (currentValue instanceof List) {
            mergedValue.addAll((List<Object>) currentValue);
        } else {
            mergedValue.add(currentValue);
        }

        mergedValue.add(value);
        setter.accept(mergedValue);
    }
}
