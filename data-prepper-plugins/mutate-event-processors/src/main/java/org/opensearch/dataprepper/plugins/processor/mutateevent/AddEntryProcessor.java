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
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyFactory;
import org.opensearch.dataprepper.model.event.JacksonEvent;
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

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.EVENT;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;

@DataPrepperPlugin(name = "add_entries", pluginType = Processor.class, pluginConfigurationType = AddEntryProcessorConfig.class)
public class AddEntryProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(AddEntryProcessor.class);
    private final List<AddEntryProcessorConfig.Entry> entries;

    private final ExpressionEvaluator expressionEvaluator;
    private final EventKeyFactory eventKeyFactory;

    @DataPrepperPluginConstructor
    public AddEntryProcessor(final PluginMetrics pluginMetrics, final AddEntryProcessorConfig config, final ExpressionEvaluator expressionEvaluator, final EventKeyFactory eventKeyFactory) {
        super(pluginMetrics);
        this.entries = config.getEntries();
        this.expressionEvaluator = expressionEvaluator;
        this.eventKeyFactory = eventKeyFactory;

        config.getEntries().forEach(entry -> {
            if (entry.getAddWhen() != null
                    && !expressionEvaluator.isValidExpressionStatement(entry.getAddWhen())) {
                throw new InvalidPluginConfigurationException(
                        String.format("add_when %s is not a valid expression statement. See https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/ for valid expression syntax", entry.getAddWhen()));
            }

            if (entry.getAddToElementWhen() != null
                    && !expressionEvaluator.isValidExpressionStatement(entry.getAddToElementWhen())) {
                throw new InvalidPluginConfigurationException(
                        String.format("add_to_element_when %s is not a valid expression statement. See https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/ for valid expression syntax", entry.getAddWhen()));
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
                        final String keyStr = entry.getKey();
                        EventKey key = null;
                        if (keyStr != null) {
                            try {
                                if (!keyStr.contains("%{") && !keyStr.contains("${")) {
                                    key = eventKeyFactory.createEventKey(keyStr);
                                } else {
                                    // Dynamic key that needs to be resolved
                                    String resolvedKey = recordEvent.formatString(keyStr, expressionEvaluator);
                                    key = eventKeyFactory.createEventKey(resolvedKey);
                                }
                            } catch (Exception e) {
                                LOG.debug("Failed to resolve or create key {} for event {}", keyStr, recordEvent, e);
                            }
                        }
                        final String metadataKey = entry.getMetadataKey();
                        final String iterateOn = entry.getIterateOn();
                        if (Objects.isNull(iterateOn)) {
                            handleWithoutIterateOn(entry, recordEvent, key, metadataKey);
                        } else if (!Objects.isNull(key) && key.getKey() != null) {
                            handleWithIterateOn(entry, recordEvent, iterateOn, key);
                        }
                    } catch (Exception e) {
                        LOG.atError()
                                .addMarker(EVENT)
                                .addMarker(NOISY)
                                .setMessage("Error adding entry to record [{}] with iterate_on [{}], add_to_element_when [{}], key [{}], metadataKey [{}], value_expression [{}] format [{}], value [{}]")
                                .addArgument(recordEvent)
                                .addArgument(entry.getIterateOn())
                                .addArgument(entry.getAddToElementWhen())
                                .addArgument(entry.getKey())
                                .addArgument(entry.getMetadataKey())
                                .addArgument(entry.getValueExpression())
                                .addArgument(entry.getFormat())
                                .addArgument(entry.getValue())
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

    private void handleWithoutIterateOn(final AddEntryProcessorConfig.Entry entry,
                                        final Event recordEvent,
                                        final EventKey key,
                                        final String metadataKey) {
        final Object value = retrieveValue(entry, recordEvent);
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
    }

    private void handleWithIterateOn(final AddEntryProcessorConfig.Entry entry,
                                     final Event recordEvent,
                                     final String iterateOn,
                                     final EventKey key) {
        final List<Map<String, Object>> iterateOnList = recordEvent.get(iterateOn, List.class);
        if (iterateOnList != null) {
            for (final Map<String, Object> item : iterateOnList) {
                final Object value;
                final Event context = JacksonEvent.builder()
                        .withEventMetadata(recordEvent.getMetadata())
                        .withData(item)
                        .build();
                if (entry.getAddToElementWhen() != null && !expressionEvaluator.evaluateConditional(entry.getAddToElementWhen(), recordEvent)) {
                    continue;
                }

                value = retrieveValue(entry, context);
                final String keyStr = key.getKey();  // Key and keyStr are guaranteed non-null by caller
                if (!item.containsKey(keyStr) || entry.getOverwriteIfKeyExists()) {
                    item.put(keyStr, value);
                } else if (item.containsKey(keyStr) && entry.getAppendIfKeyExists()) {
                    mergeValueToMap(item, keyStr, value);
                }
            }
            recordEvent.put(iterateOn, iterateOnList);
        }
    }

    private Object retrieveValue(final AddEntryProcessorConfig.Entry entry,
                                 final Event context) {
        Object value;
        if (!Objects.isNull(entry.getValueExpression())) {
            value = expressionEvaluator.evaluate(entry.getValueExpression(), context);
        } else if (!Objects.isNull(entry.getFormat())) {
            try {
                value = context.formatString(entry.getFormat());
            } catch (final EventKeyNotFoundException e) {
                value = null;
            }
        } else {
            value = entry.getValue();
        }
        return value;
    }

    private void mergeValueToEvent(final Event recordEvent, final EventKey key, final Object value) {
        mergeValue(value, () -> recordEvent.get(key, Object.class), newValue -> recordEvent.put(key, newValue));
    }

    private void mergeValueToEventMetadata(final Event recordEvent, final String key, final Object value) {
        mergeValue(value, () -> recordEvent.getMetadata().getAttribute(key), newValue -> recordEvent.getMetadata().setAttribute(key, newValue));
    }

    private void mergeValueToMap(final Map<String, Object> item, final String key, final Object value) {
        mergeValue(value, () -> item.get(key), newValue -> item.put(key, newValue));
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
