/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.parsejson;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.annotations.DataPrepperPluginConstructor;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.event.JacksonEvent;
import com.amazon.dataprepper.model.processor.AbstractProcessor;
import com.amazon.dataprepper.model.processor.Processor;
import com.amazon.dataprepper.model.record.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@DataPrepperPlugin(name = "parse_json", pluginType = Processor.class, pluginConfigurationType = ParseJsonProcessorConfig.class)
public class ParseJsonProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(ParseJsonProcessor.class);

    private final String source;
    private final String destination;
    private final String pointer;

    @DataPrepperPluginConstructor
    public ParseJsonProcessor(final PluginMetrics pluginMetrics, final ParseJsonProcessorConfig parseJsonProcessorConfig) {
        super(pluginMetrics);

        source = parseJsonProcessorConfig.getSource();
        destination = parseJsonProcessorConfig.getDestination();
        pointer = parseJsonProcessorConfig.getPointer();
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        final ObjectMapper objectMapper = new ObjectMapper();
        final boolean doWriteToRoot = Objects.isNull(destination);
        final boolean doUsePointer = Objects.nonNull(pointer);

        for (final Record<Event> record : records) {
            final Event event = record.getData();
            final String message = event.get(source, String.class);
            try {
                final TypeReference<HashMap<String, Object>> hashMapTypeReference = new TypeReference<HashMap<String, Object>>() {};
                Map<String, Object> parsedJson = objectMapper.readValue(message, hashMapTypeReference);

                if (doUsePointer) {
                    parsedJson = parseUsingPointer(event, parsedJson, pointer, doWriteToRoot);
                }

                if (doWriteToRoot) {
                    writeToRoot(event, parsedJson);
                } else {
                    event.put(destination, parsedJson);
                }
            } catch (final JsonProcessingException jsonException) {
                LOG.error("An exception occurred due to invalid JSON while reading event [{}]", event, jsonException);
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

    private Map<String, Object> parseUsingPointer(final Event event, final Map<String, Object> parsedJson, final String pointer,
                                                  final boolean doWriteToRoot) {
        final Event temporaryEvent = JacksonEvent.builder().withEventType("event").build();
        temporaryEvent.put(source, parsedJson);

        final String trimmedPointer = trimPointer(pointer);
        final String actualPointer = source + "/" + trimmedPointer;

        final boolean pointerIsValid = temporaryEvent.containsKey(actualPointer);
        if (!pointerIsValid) {
            LOG.error("Writing entire JSON because the pointer {} is invalid on Event {}", pointer, event);
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
            final String lastTwoElements = elements.get(elements.size() - 2) + "/" + elements.get(elements.size() - 1);
            return lastTwoElements;
        }

        return elements.get(elements.size()-1);
    }

    /**
     * Trim the pointer and change each front slash / to be a dot (.) to proccess
     * @param pointer
     * @return
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
            event.put(entry.getKey(), entry.getValue());
        }
    }
}