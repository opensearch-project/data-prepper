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
    private static final String ERROR_LOG_MESSAGE = "Error adding entry to record [{}] with iterate_on [{}], add_to_element_when [{}], key [{}], metadataKey [{}], value_expression [{}] format [{}], value [{}]";
    private final List<AddEntryProcessorConfig.Entry> entries;
    private final List<EntryProperties> entryProperties;
    private final List<KeyInfo> preprocessedKeys;
    private final ExpressionEvaluator expressionEvaluator;
    private final EventKeyFactory eventKeyFactory;
    private static final Class<List<Map<String,Object>>> ITERATE_LIST_CLASS = (Class<List<Map<String,Object>>>) (Class<?>) List.class;

    private static class EntryProperties {
        final boolean overwriteIfExists;
        final boolean appendIfExists;
        final String addWhen;
        final String addToElementWhen;
        final Object staticExpressionValue;

        EntryProperties(AddEntryProcessorConfig.Entry entry, ExpressionEvaluator evaluator) {
            this.overwriteIfExists = entry.getOverwriteIfKeyExists();
            this.appendIfExists = entry.getAppendIfKeyExists();
            this.addWhen = entry.getAddWhen();
            this.addToElementWhen = entry.getAddToElementWhen();
            String valueExpr = entry.getValueExpression();
            this.staticExpressionValue = (valueExpr != null && !containsEventReference(valueExpr)) ? 
                evaluator.evaluate(valueExpr, null) : null;
        }

        private boolean containsEventReference(String expression) {
            return expression.contains("/") || expression.contains("getMetadata");
        }
    }

    private static class KeyInfo {
        private final String keyStr;
        private final boolean isDynamic;
        private final EventKey staticKey;
        private final boolean addWhenEvaluated;
        private final String[] formatParts;

        KeyInfo(String keyStr, EventKeyFactory factory, String addWhen, String format) {
            this.keyStr = keyStr;
            this.isDynamic = keyStr != null && (keyStr.contains("%{") || keyStr.contains("${"));
            this.staticKey = !this.isDynamic && keyStr != null ? factory.createEventKey(keyStr) : null;
            this.addWhenEvaluated = addWhen == null;
            this.formatParts = format != null ? parseFormat(format) : null;
        }

        private String[] parseFormat(String format) {
            if (!isValidFormat(format)) {
                return null;
            }
            List<String> parts = new ArrayList<>();
            StringBuilder current = new StringBuilder();
            int i = 0;
            while (i < format.length()) {
                if (format.charAt(i) == '$' && i + 1 < format.length() && format.charAt(i + 1) == '{') {
                    if (current.length() > 0) {
                        parts.add(current.toString());
                        current = new StringBuilder();
                    }
                    int end = format.indexOf('}', i);
                    if (end == -1) break;
                    parts.add(format.substring(i, end + 1));
                    i = end + 1;
                } else {
                    current.append(format.charAt(i));
                    i++;
                }
            }
            if (current.length() > 0) {
                parts.add(current.toString());
            }
            return parts.toArray(new String[0]);
        }

        private boolean isValidFormat(String format) {
            if (format == null) return false;
            int count = 0;
            for (int i = 0; i < format.length(); i++) {
                if (format.charAt(i) == '$' && i + 1 < format.length() && format.charAt(i + 1) == '{') {
                    count++;
                    int end = format.indexOf('}', i);
                    if (end == -1) return false;
                    i = end;
                }
            }
            return count > 0;
        }
    }

    @DataPrepperPluginConstructor
    public AddEntryProcessor(final PluginMetrics pluginMetrics, final AddEntryProcessorConfig config, final ExpressionEvaluator expressionEvaluator, final EventKeyFactory eventKeyFactory) {
        super(pluginMetrics);
        this.entries = config.getEntries();
        this.expressionEvaluator = expressionEvaluator;
        this.eventKeyFactory = eventKeyFactory;
        this.preprocessedKeys = new ArrayList<>(entries.size());
        this.entryProperties = new ArrayList<>(entries.size());
        
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
            
            preprocessedKeys.add(new KeyInfo(entry.getKey(), eventKeyFactory, entry.getAddWhen(), entry.getFormat()));
            entryProperties.add(new EntryProperties(entry, expressionEvaluator));
        });
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for(final Record<Event> record : records) {
            final Event recordEvent = record.getData();

            try {
                for (int i = 0; i < entries.size(); i++) {
                    AddEntryProcessorConfig.Entry entry = entries.get(i);
                    KeyInfo keyInfo = preprocessedKeys.get(i);
                    EntryProperties props = entryProperties.get(i);

                    if (Objects.nonNull(props.addWhen) && !expressionEvaluator.evaluateConditional(props.addWhen, recordEvent)) {
                        continue;
                    }

                    try {
                        EventKey key = null;
                        if (keyInfo.keyStr != null) {
                            try {
                                key = keyInfo.isDynamic ?
                                    eventKeyFactory.createEventKey(recordEvent.formatString(keyInfo.keyStr, expressionEvaluator)) :
                                    keyInfo.staticKey;
                            } catch (Exception e) {
                                LOG.debug("Failed to resolve or create key {} for event {}", keyInfo.keyStr, recordEvent, e);
                            }
                        }
                        final String metadataKey = entry.getMetadataKey();
                        final String iterateOn = entry.getIterateOn();
                        final boolean flattenKey = entry.getFlattenKey();
                        if (Objects.isNull(iterateOn)) {
                            handleWithoutIterateOn(entry, recordEvent, key, metadataKey, props);
                        } else if (!Objects.isNull(key) && key.getKey() != null) {
                            handleWithIterateOn(entry, recordEvent, iterateOn, flattenKey, key, props);
                        }
                    } catch (Exception e) {
                        LOG.atError()
                                .addMarker(EVENT)
                                .addMarker(NOISY)
                                .setMessage(ERROR_LOG_MESSAGE)
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
                                    final String metadataKey,
                                    final EntryProperties props) {
        final Object value = retrieveValue(entry, recordEvent);
        if (!Objects.isNull(key)) {
            final boolean keyExists = recordEvent.containsKey(key);
            if (!keyExists || props.overwriteIfExists) {
                recordEvent.put(key, value);
            } else if (keyExists && props.appendIfExists) {
                mergeValueToEvent(recordEvent, key, value);
            }
        } else {
            Map<String, Object> attributes = recordEvent.getMetadata().getAttributes();
            if (!attributes.containsKey(metadataKey) || props.overwriteIfExists) {
                recordEvent.getMetadata().setAttribute(metadataKey, value);
            } else if (attributes.containsKey(metadataKey) && props.appendIfExists) {
                mergeValueToEventMetadata(recordEvent, metadataKey, value);
            }
        }
    }

    private void handleWithIterateOn(final AddEntryProcessorConfig.Entry entry,
                                     final Event recordEvent,
                                     final String iterateOn,
                                     final boolean flattenKey,
                                     final EventKey key,
                                     final EntryProperties props) {
        final List<Map<String, Object>> iterateOnList = recordEvent.get(iterateOn, ITERATE_LIST_CLASS);
        if (iterateOnList != null && !iterateOnList.isEmpty()) {
            // Create builder once
            final JacksonEvent.Builder contextBuilder = JacksonEvent.builder()
                    .withEventMetadata(recordEvent.getMetadata());
            
            // Pre-check addToElementWhen condition if static
            final boolean shouldProcessAll = props.addToElementWhen == null || 
                                      expressionEvaluator.evaluateConditional(props.addToElementWhen, recordEvent);
            if (shouldProcessAll) {
                // Bulk process all items
                for (int i = 0; i < iterateOnList.size(); i++) {
                    final Map<String, Object> item = iterateOnList.get(i);
                    iterateOnList.set(i, processIterateOnItem(entry, contextBuilder, item, flattenKey, key, props));
                }
            } else {
                // Process items individually with condition check
                for (int i = 0; i < iterateOnList.size(); i++) {
                    final Map<String, Object> item = iterateOnList.get(i);
                    if (expressionEvaluator.evaluateConditional(props.addToElementWhen, recordEvent)) {
                        iterateOnList.set(i, processIterateOnItem(entry, contextBuilder, item, flattenKey, key, props));
                    }
                }
            }
            recordEvent.put(iterateOn, iterateOnList);
        }
    }

    private Map<String, Object> processIterateOnItem(AddEntryProcessorConfig.Entry entry, JacksonEvent.Builder contextBuilder, 
                                    Map<String, Object> item, final boolean flattenKey, EventKey key, EntryProperties props) {
        final Event context = contextBuilder.withData(item).build();
        final Object value = retrieveValue(entry, context);
        final String keyStr = key.getKey();  // Key and keyStr are guaranteed non-null by caller
        if (!item.containsKey(keyStr) || props.overwriteIfExists) {
            if (flattenKey) {
                item.put(keyStr, value);
            }  else {
                context.put(key, value);
            }
        } else if (item.containsKey(keyStr) && props.appendIfExists) {
            if (flattenKey) {
                mergeValueToMap(item, keyStr, value);
            }  else {
                mergeValueToEvent(context, key, value);
            }
        }
        if (flattenKey){
            return item;
        }
        return context.toMap();
    }

    private Object retrieveValue(final AddEntryProcessorConfig.Entry entry, final Event context) {
        Object value;
        int entryIndex = entries.indexOf(entry);
        EntryProperties props = entryProperties.get(entryIndex);
        KeyInfo keyInfo = preprocessedKeys.get(entryIndex);
        
        if (!Objects.isNull(entry.getValueExpression())) {
            value = props.staticExpressionValue != null ? 
                    props.staticExpressionValue : 
                    expressionEvaluator.evaluate(entry.getValueExpression(), context);
        } else if (!Objects.isNull(entry.getFormat())) {
            try {
                if (keyInfo.formatParts != null) {
                    StringBuilder result = new StringBuilder();
                    for (String part : keyInfo.formatParts) {
                        if (part.startsWith("${")) {
                            String key = part.substring(2, part.length() - 1);
                            try {
                                Object partValue = context.get(key, Object.class);
                                if (partValue != null) {
                                    result.append(partValue);
                                }
                            } catch (EventKeyNotFoundException ignored) {}
                        } else {
                            result.append(part);
                        }
                    }
                    value = result.toString();
                } else {
                    value = context.formatString(entry.getFormat());
                }
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
        final List<Object> mergedValue;
        if (currentValue instanceof List) {
            mergedValue = new ArrayList<>((List<Object>)currentValue);
        } else {
            mergedValue = new ArrayList<>(2);
            mergedValue.add(currentValue);
        }
        
        if (value instanceof List) {
            mergedValue.addAll((List<Object>)value);
        } else {
            mergedValue.add(value);
        }
        setter.accept(mergedValue);
    }
}