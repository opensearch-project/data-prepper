/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.parse;

import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.EVENT;

public abstract class AbstractParseProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractParseProcessor.class);

    private final String source;
    private final String destination;
    private final String pointer;
    private final String parseWhen;
    private final List<String> tagsOnFailure;
    private final boolean overwriteIfDestinationExists;
    private final boolean deleteSourceRequested;

    private final ExpressionEvaluator expressionEvaluator;

    protected AbstractParseProcessor(PluginMetrics pluginMetrics,
                                     CommonParseConfig commonParseConfig,
                                     ExpressionEvaluator expressionEvaluator) {
        super(pluginMetrics);

        source = commonParseConfig.getSource();
        destination = commonParseConfig.getDestination();
        pointer = commonParseConfig.getPointer();
        parseWhen = commonParseConfig.getParseWhen();
        tagsOnFailure = commonParseConfig.getTagsOnFailure();
        overwriteIfDestinationExists = commonParseConfig.getOverwriteIfDestinationExists();
        deleteSourceRequested = commonParseConfig.isDeleteSourceRequested();
        this.expressionEvaluator = expressionEvaluator;
    }

    /**
     * @return Optional HashMap of the parsed value - empty if the message was invalid (be sure to log the error)
     */
    protected abstract Optional<HashMap<String, Object>> readValue(String message, Event context);

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        final boolean doWriteToRoot = Objects.isNull(destination);
        final boolean doUsePointer = Objects.nonNull(pointer);

        for (final Record<Event> record : records) {
            try {
                final Event event = record.getData();

                if (Objects.nonNull(parseWhen) && !expressionEvaluator.evaluateConditional(parseWhen, event)) {
                    continue;
                }

                final String message = event.get(source, String.class);
                if (Objects.isNull(message) && !doUsePointer) {
                    continue;
                }

                final Optional<HashMap<String, Object>> parsedValueOptional = readValue(message, event);
                if (parsedValueOptional.isEmpty()) {
                    event.getMetadata().addTags(tagsOnFailure);
                    continue;
                }

                Map<String, Object> parsedValue = parsedValueOptional.get();

                if (doUsePointer) {
                    parsedValue = parseUsingPointer(event, parsedValue, pointer, doWriteToRoot);
                }

                if (doWriteToRoot) {
                    writeToRoot(event, parsedValue);
                } else if (overwriteIfDestinationExists || !event.containsKey(destination)) {
                    event.put(destination, parsedValue);
                }

                if(deleteSourceRequested) {
                    event.delete(this.source);
                }
            } catch (Exception e) {
                LOG.error(EVENT, "An exception occurred while using the {} processor on Event [{}]", getProcessorName(), record.getData(), e);
            }
        }
        return records;
    }

    @Override
    public void prepareForShutdown() {
        /* nothing to do */
    }

    @Override
    public boolean isReadyForShutdown() {
        return true;
    }

    @Override
    public void shutdown() {
        /* nothing to do */
    }

    private String getProcessorName() {
        return this.getClass().getAnnotation(DataPrepperPlugin.class).name();
    }

    private Map<String, Object> parseUsingPointer(final Event event, final Map<String, Object> parsedJson, final String pointer,
                                                  final boolean doWriteToRoot) {
        final Event temporaryEvent = JacksonEvent.builder().withEventType("event").build();
        temporaryEvent.put(source, parsedJson);

        final String trimmedPointer = trimPointer(pointer);
        final String actualPointer = source + "/" + trimmedPointer;

        final boolean pointerIsValid = temporaryEvent.containsKey(actualPointer);
        if (!pointerIsValid) {
            LOG.error(EVENT, "Writing entire source because the pointer {} is invalid on Event [{}]", pointer, event);
            return parsedJson;
        }

        final Object valueAtPointer = temporaryEvent.get(actualPointer, Object.class);
        final String endOfPointer = getEndOfPointer(trimmedPointer);

        final boolean shouldUseEntirePointerAsKey = event.containsKey(endOfPointer) && doWriteToRoot;
        if (shouldUseEntirePointerAsKey) {
            return Collections.singletonMap(normalizePointerStructure(trimmedPointer), valueAtPointer);
        }

        return Collections.singletonMap(normalizePointerStructure(endOfPointer), valueAtPointer);
    }

    private String getEndOfPointer(final String trimmedPointer) {
        final ArrayList<String> elements = new ArrayList<>(Arrays.asList(trimmedPointer.split("/")));
        if (elements.size() <= 1) return trimmedPointer;

        final boolean lastElementInPathIsAnArrayIndex = elements.get(elements.size()-1).matches("[0-9]+");

        if (lastElementInPathIsAnArrayIndex) {
            return elements.get(elements.size() - 2) + "/" + elements.get(elements.size() - 1); // return the last two elements
        }

        return elements.get(elements.size()-1);
    }

    /**
     * Trim the pointer and change each front slash / to be a dot (.) to process
     */
    private String normalizePointerStructure(final String pointer) {
        return pointer.replace('/','.');
    }

    private String trimPointer(String pointer) {
        final String trimmedLeadingSlash = pointer.startsWith("/") ? pointer.substring(1) : pointer;
        return trimmedLeadingSlash.endsWith("/") ? trimmedLeadingSlash.substring(0, trimmedLeadingSlash.length() - 1) : trimmedLeadingSlash;
    }

    private void writeToRoot(final Event event, final Map<String, Object> parsedJson) {
        for (Map.Entry<String, Object> entry : parsedJson.entrySet()) {
            if (overwriteIfDestinationExists || !event.containsKey(entry.getKey())) {
                event.put(entry.getKey(), entry.getValue());
            }
        }
    }
}
