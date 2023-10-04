/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.ion.IonObjectMapper;
import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.dynamodb.model.TableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ExportRecordConverter extends RecordConverter {

    private static final Logger LOG = LoggerFactory.getLogger(ExportRecordConverter.class);

    private static final String ITEM_KEY = "Item";

    static final String EXPORT_RECORD_SUCCESS_COUNT = "exportRecordSuccess";
    static final String EXPORT_RECORD_ERROR_COUNT = "exportRecordErrors";


    IonObjectMapper MAPPER = new IonObjectMapper();

    private final PluginMetrics pluginMetrics;

    private final Counter exportRecordSuccessCounter;
    private final Counter exportRecordErrorCounter;

    public ExportRecordConverter(Buffer<Record<Event>> buffer, TableInfo tableInfo, PluginMetrics pluginMetrics) {
        super(buffer, tableInfo);
        this.pluginMetrics = pluginMetrics;
        this.exportRecordSuccessCounter = pluginMetrics.counter(EXPORT_RECORD_SUCCESS_COUNT);
        this.exportRecordErrorCounter = pluginMetrics.counter(EXPORT_RECORD_ERROR_COUNT);

    }

    private Map<String, Object> convertToMap(String jsonData) {
        try {
            return MAPPER.readValue(jsonData, Map.class);
        } catch (JsonProcessingException e) {
            return null;
        }
    }


    @Override
    String getEventType() {
        return "EXPORT";
    }

    public void writeToBuffer(List<String> lines) {
        List<Map<String, Object>> data = lines.stream()
                .map(this::convertToMap)
                .map(d -> (Map<String, Object>) d.get(ITEM_KEY))
                .collect(Collectors.toList());

        List<Record<Event>> events = data.stream().map(this::convertToEvent).collect(Collectors.toList());

        try {
            writeEventsToBuffer(events);
            exportRecordSuccessCounter.increment(events.size());
        } catch (Exception e) {
            LOG.error("Failed to write {} events to buffer due to {}", events.size(), e.getMessage());
            exportRecordErrorCounter.increment(events.size());
        }
    }


}
