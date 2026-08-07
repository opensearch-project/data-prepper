/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.parse;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyFactory;
import org.opensearch.dataprepper.model.event.HandleFailedEventsOption;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.EVENT;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;
import static org.opensearch.dataprepper.logging.DataPrepperMarkers.SENSITIVE;
import org.opensearch.dataprepper.model.record.Record;
import io.micrometer.core.instrument.Counter;
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
    private static final String PROCESSING_FAILURES = "processingFailures";

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final EventKey source;
    private final EventKey destination;
    private final String pointer;
    private final String parseWhen;
    private final List<String> tagsOnFailure;
    private final boolean overwriteIfDestinationExists;
    private final boolean deleteSourceRequested;

    private final HandleFailedEventsOption handleFailedEventsOption;

    protected final Counter processingFailuresCounter;

    private final ExpressionEvaluator expressionEvaluator;
    private final EventKeyFactory eventKeyFactory;
    private final boolean normalizeKeys;

    protected AbstractParseProcessor(final PluginMetrics pluginMetrics,
                                     final CommonParseConfig commonParseConfig,
                                     final ExpressionEvaluator expressionEvaluator,
                                     final EventKeyFactory eventKeyFactory) {
        super(pluginMetrics);

        source = eventKeyFactory.createEventKey(commonParseConfig.getSource(), EventKeyFactory.EventAction.GET, EventKeyFactory.EventAction.DELETE);
        destination = commonParseConfig.getDestination() != null ? eventKeyFactory.createEventKey(commonParseConfig.getDestination(), EventKeyFactory.EventAction.PUT, EventKeyFactory.EventAction.GET) : null;
        pointer = commonParseConfig.getPointer();
        parseWhen = commonParseConfig.getParseWhen();
        tagsOnFailure = commonParseConfig.getTagsOnFailure();
        overwriteIfDestinationExists = commonParseConfig.getOverwriteIfDestinationExists();
        deleteSourceRequested = commonParseConfig.isDeleteSourceRequested();
        handleFailedEventsOption = commonParseConfig.getHandleFailedEventsOption();
        processingFailuresCounter = pluginMetrics.counter(PROCESSING_FAILURES);
        normalizeKeys = commonParseConfig.getNormalizeKeys();
        this.expressionEvaluator = expressionEvaluator;
        this.eventKeyFactory = eventKeyFactory;

        if (commonParseConfig.getParseWhen() != null
                && !expressionEvaluator.isValidExpressionStatement(commonParseConfig.getParseWhen())) {
            throw new InvalidPluginConfigurationException(
                    String.format("parse_when value of %s is not a valid expression statement. " +
                            "See https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/ for valid expression syntax.", commonParseConfig.getParseWhen()));
        }
    }

    /**
     * @param message message
     * @param context context
     * @return Optional HashMap of the parsed value - empty if the message was invalid (be sure to log the error)
     */
    protected abstract Optional<Map<String, Object>> readValue(String message, Event context);

    protected Map<String, Object> convertNestedObjectToString(Map<String, Object> map, int curDepth, int targetDepth) {
        HashMap<String, Object> resultMap = new HashMap<>();
        try {
            for (Map.Entry<String, Object> entry: map.entrySet()) {
                if (entry.getValue() instanceof Map) {
                    if (curDepth == targetDepth) {
                        resultMap.put(entry.getKey(), objectMapper.writeValueAsString((Map<String, Object>) entry.getValue()));
                    } else {
                        resultMap.put(entry.getKey(), convertNestedObjectToString((Map<String, Object>) entry.getValue(), curDepth + 1, targetDepth));
                    }
                } else {
                    resultMap.put(entry.getKey(), entry.getValue());
                }
            }
        } catch (Exception e) {
            LOG.atError()
                        .addMarker(SENSITIVE)
                        .addMarker(NOISY)
                        .setMessage("Failed to convert to string while parsing")
                        .setCause(e)
                        .log();
        }
        return resultMap;
    }

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

                final Optional<Map<String, Object>> parsedValueOptional = readValue(message, event);
                if (parsedValueOptional.isEmpty()) {
                    event.getMetadata().addTags(tagsOnFailure);
                    continue;
                }

                Map<String, Object> parsedValue = parsedValueOptional.get();

                if (doUsePointer) {
                    parsedValue = parseUsingPointer(event, parsedValue, pointer, doWriteToRoot);
                }

                if (doWriteToRoot && deleteSourceRequested) {
                    event.delete(this.source);
                }

                if (doWriteToRoot) {
                    writeToRoot(event, parsedValue);
                } else if (overwriteIfDestinationExists || !event.containsKey(destination)) {
                    event.put(destination, parsedValue, normalizeKeys);
                }

                if (deleteSourceRequested && !doWriteToRoot) {
                    event.delete(this.source);
                }
            } catch (Exception e) {
                processingFailuresCounter.increment();
                if (handleFailedEventsOption.shouldLog()) {
                    LOG.error(EVENT, "An exception occurred while using the {} processor on Event [{}]", getProcessorName(), record.getData(), e);
                }
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
        final EventKey temporaryPutKey = eventKeyFactory.createEventKey(source.getKey(), EventKeyFactory.EventAction.PUT);
        temporaryEvent.put(temporaryPutKey, parsedJson, normalizeKeys);

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
     * @param pointer input string
     * @return returns replaced string
     */
    private String normalizePointerStructure(final String pointer) {
        return pointer.replace('/','.');
    }

    private String trimPointer(final String pointer) {
        final String trimmedLeadingSlash = pointer.startsWith("/") ? pointer.substring(1) : pointer;
        return trimmedLeadingSlash.endsWith("/") ? trimmedLeadingSlash.substring(0, trimmedLeadingSlash.length() - 1) : trimmedLeadingSlash;
    }

    private void writeToRoot(final Event event, final Map<String, Object> parsedJson) {
        for (final Map.Entry<String, Object> entry : parsedJson.entrySet()) {
            if (overwriteIfDestinationExists || !event.containsKey(entry.getKey())) {
                event.put(eventKeyFactory.createEventKey(entry.getKey(), EventKeyFactory.EventAction.PUT), entry.getValue(), normalizeKeys);
            }
        }
    }
}
