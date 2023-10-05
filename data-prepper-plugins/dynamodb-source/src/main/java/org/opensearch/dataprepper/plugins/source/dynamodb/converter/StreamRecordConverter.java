/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.enhanced.dynamodb.document.EnhancedDocument;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StreamRecordConverter extends RecordConverter {
    private static final Logger LOG = LoggerFactory.getLogger(StreamRecordConverter.class);


    static final String CHANGE_EVENT_SUCCESS_COUNT = "changeEventSuccess";
    static final String CHANGE_EVENT_ERROR_COUNT = "changeEventErrors";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<Map<String, Object>>() {
    };

    private final PluginMetrics pluginMetrics;

    private final Counter changeEventSuccessCounter;
    private final Counter changeEventErrorCounter;

    public StreamRecordConverter(Buffer<Record<Event>> buffer, TableInfo tableInfo, PluginMetrics pluginMetrics) {
        super(buffer, tableInfo);
        this.pluginMetrics = pluginMetrics;
        this.changeEventSuccessCounter = pluginMetrics.counter(CHANGE_EVENT_SUCCESS_COUNT);
        this.changeEventErrorCounter = pluginMetrics.counter(CHANGE_EVENT_ERROR_COUNT);
    }

    @Override
    String getEventType() {
        return "STREAM";
    }

    public void writeToBuffer(List<software.amazon.awssdk.services.dynamodb.model.Record> records) {
        // TODO: What if convert failed.
        List<Record<Event>> events = records.stream()
                .map(record -> convertToEvent(
                        toMap(EnhancedDocument.fromAttributeValueMap(record.dynamodb().newImage()).toJson()),
                        record.dynamodb().approximateCreationDateTime(),
                        record.eventNameAsString()))
                .collect(Collectors.toList());

        try {
            writeEventsToBuffer(events);
            changeEventSuccessCounter.increment(events.size());
        } catch (Exception e) {
            LOG.error("Failed to write {} events to buffer due to {}", events.size(), e.getMessage());
            changeEventErrorCounter.increment(events.size());
        }
    }


    private Map<String, Object> toMap(String jsonData) {
        try {
            return MAPPER.readValue(jsonData, MAP_TYPE_REFERENCE);
        } catch (JsonProcessingException e) {
            return null;
        }
    }
}
