/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.enhanced.dynamodb.document.EnhancedDocument;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.Record;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StreamRecordConverter extends RecordConverter {
    private static final Logger LOG = LoggerFactory.getLogger(StreamRecordConverter.class);


    static final String CHANGE_EVENTS_PROCESSED_COUNT = "changeEventsProcessed";
    static final String CHANGE_EVENTS_PROCESSING_ERROR_COUNT = "changeEventsProcessingErrors";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<Map<String, Object>>() {
    };

    private final PluginMetrics pluginMetrics;

    private final Counter changeEventSuccessCounter;
    private final Counter changeEventErrorCounter;

    public StreamRecordConverter(final BufferAccumulator<org.opensearch.dataprepper.model.record.Record<Event>> bufferAccumulator, TableInfo tableInfo, PluginMetrics pluginMetrics) {
        super(bufferAccumulator, tableInfo);
        this.pluginMetrics = pluginMetrics;
        this.changeEventSuccessCounter = pluginMetrics.counter(CHANGE_EVENTS_PROCESSED_COUNT);
        this.changeEventErrorCounter = pluginMetrics.counter(CHANGE_EVENTS_PROCESSING_ERROR_COUNT);
    }

    @Override
    String getEventType() {
        return "STREAM";
    }


    public void writeToBuffer(final AcknowledgementSet acknowledgementSet, List<Record> records) {

        int eventCount = 0;
        for (Record record : records) {
            // NewImage may be empty
            Map<String, Object> data = convertData(record.dynamodb().newImage());
            // Always get keys from dynamodb().keys()
            Map<String, Object> keys = convertKeys(record.dynamodb().keys());

            try {
                addToBuffer(acknowledgementSet, data, keys, record.dynamodb().approximateCreationDateTime().toEpochMilli(), record.eventNameAsString());
                eventCount++;
            } catch (Exception e) {
                // will this cause too many logs?
                LOG.error("Failed to add event to buffer due to {}", e.getMessage());
                changeEventErrorCounter.increment();
            }
        }

        try {
            flushBuffer();
            changeEventSuccessCounter.increment(eventCount);
        } catch (Exception e) {
            LOG.error("Failed to write {} events to buffer due to {}", eventCount, e.getMessage());
            changeEventErrorCounter.increment(eventCount);
        }
    }


    /**
     * Convert the DynamoDB attribute map to a normal map for data
     */
    private Map<String, Object> convertData(Map<String, AttributeValue> data) {
        try {
            String jsonData = EnhancedDocument.fromAttributeValueMap(data).toJson();
            return MAPPER.readValue(jsonData, MAP_TYPE_REFERENCE);
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    /**
     * Convert the DynamoDB attribute map to a normal map for keys
     * This method may not be necessary, can use convertData() alternatively
     */
    private Map<String, Object> convertKeys(Map<String, AttributeValue> keys) {
        Map<String, Object> result = new HashMap<>();
        // The attribute type for key can only be N, B or S
        keys.forEach(((attributeName, attributeValue) -> {
            if (attributeValue.type() == AttributeValue.Type.N) {
                // N for number
                result.put(attributeName, attributeValue.n());
            } else if (attributeValue.type() == AttributeValue.Type.B) {
                // B for Binary
                result.put(attributeName, attributeValue.b().toString());
            } else {
                result.put(attributeName, attributeValue.s());
            }
        }));
        return result;

    }
}
