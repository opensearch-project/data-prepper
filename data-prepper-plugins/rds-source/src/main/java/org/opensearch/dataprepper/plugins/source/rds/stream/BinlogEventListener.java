/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventMetadata;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.opensearch.dataprepper.plugins.source.rds.converter.StreamRecordConverter;
import org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class BinlogEventListener implements BinaryLogClient.EventListener {

    private static final Logger LOG = LoggerFactory.getLogger(BinlogEventListener.class);

    static final Duration BUFFER_TIMEOUT = Duration.ofSeconds(60);
    static final int DEFAULT_BUFFER_BATCH_SIZE = 1_000;
    static final String CHANGE_EVENTS_PROCESSED_COUNT = "changeEventsProcessed";
    static final String CHANGE_EVENTS_PROCESSING_ERROR_COUNT = "changeEventsProcessingErrors";
    static final String BYTES_RECEIVED = "bytesReceived";
    static final String BYTES_PROCESSED = "bytesProcessed";

    /**
     * TableId to TableMetadata mapping
     */
    private final Map<Long, TableMetadata> tableMetadataMap;

    private final StreamRecordConverter recordConverter;
    private final BufferAccumulator<Record<Event>> bufferAccumulator;
    private final List<String> tableNames;
    private final String s3Prefix;
    private final PluginMetrics pluginMetrics;

    private final Counter changeEventSuccessCounter;
    private final Counter changeEventErrorCounter;
    private final DistributionSummary bytesReceivedSummary;
    private final DistributionSummary bytesProcessedSummary;

    public BinlogEventListener(final Buffer<Record<Event>> buffer,
                               final RdsSourceConfig sourceConfig,
                               final PluginMetrics pluginMetrics) {
        tableMetadataMap = new HashMap<>();
        recordConverter = new StreamRecordConverter(sourceConfig.getStream().getPartitionCount());
        bufferAccumulator = BufferAccumulator.create(buffer, DEFAULT_BUFFER_BATCH_SIZE, BUFFER_TIMEOUT);
        s3Prefix = sourceConfig.getS3Prefix();
        tableNames = sourceConfig.getTableNames();
        this.pluginMetrics = pluginMetrics;

        changeEventSuccessCounter = pluginMetrics.counter(CHANGE_EVENTS_PROCESSED_COUNT);
        changeEventErrorCounter = pluginMetrics.counter(CHANGE_EVENTS_PROCESSING_ERROR_COUNT);
        bytesReceivedSummary = pluginMetrics.summary(BYTES_RECEIVED);
        bytesProcessedSummary = pluginMetrics.summary(BYTES_PROCESSED);
    }

    @Override
    public void onEvent(com.github.shyiko.mysql.binlog.event.Event event) {
        EventType eventType = event.getHeader().getEventType();

        switch (eventType) {
            case TABLE_MAP:
                handleEventAndErrors(event, this::handleTableMapEvent);
                break;
            case WRITE_ROWS:
            case EXT_WRITE_ROWS:
                handleEventAndErrors(event, this::handleInsertEvent);
                break;
            case UPDATE_ROWS:
            case EXT_UPDATE_ROWS:
                handleEventAndErrors(event, this::handleUpdateEvent);
                break;
            case DELETE_ROWS:
            case EXT_DELETE_ROWS:
                handleEventAndErrors(event, this::handleDeleteEvent);
                break;
        }
    }

    void handleTableMapEvent(com.github.shyiko.mysql.binlog.event.Event event) {
        final TableMapEventData data = event.getData();
        final TableMapEventMetadata tableMapEventMetadata = data.getEventMetadata();
        final List<String> columnNames = tableMapEventMetadata.getColumnNames();
        final List<String> primaryKeys = tableMapEventMetadata.getSimplePrimaryKeys().stream()
                .map(columnNames::get)
                .collect(Collectors.toList());
        final TableMetadata tableMetadata = new TableMetadata(
                data.getTable(), data.getDatabase(), columnNames, primaryKeys);
        if (isTableOfInterest(tableMetadata.getFullTableName())) {
            tableMetadataMap.put(data.getTableId(), tableMetadata);
        }
    }

    void handleInsertEvent(com.github.shyiko.mysql.binlog.event.Event event) {
        final long bytes = event.toString().getBytes().length;
        bytesReceivedSummary.record(bytes);

        LOG.debug("Handling insert event");
        final WriteRowsEventData data = event.getData();
        if (!tableMetadataMap.containsKey(data.getTableId())) {
            LOG.debug("Cannot find table metadata, the event is likely not from a table of interest or the table metadata was not read");
            return;
        }
        final TableMetadata tableMetadata = tableMetadataMap.get(data.getTableId());
        final String fullTableName = tableMetadata.getFullTableName();
        if (!isTableOfInterest(fullTableName)) {
            LOG.debug("The event is not from a table of interest");
            return;
        }
        final List<String> columnNames = tableMetadata.getColumnNames();
        final List<String> primaryKeys = tableMetadata.getPrimaryKeys();
        final long eventTimestampMillis = event.getHeader().getTimestamp();

        // Construct data prepper JacksonEvent
        int eventCount = 0;
        for (final Object[] rowDataArray : data.getRows()) {
            final Map<String, Object> rowDataMap = new HashMap<>();
            for (int i = 0; i < rowDataArray.length; i++) {
                rowDataMap.put(columnNames.get(i), rowDataArray[i]);
            }

            Event pipelineEvent = recordConverter.convert(
                    rowDataMap,
                    tableMetadata.getDatabaseName(),
                    tableMetadata.getTableName(),
                    event.getHeader().getEventType(),
                    OpenSearchBulkActions.INDEX,
                    primaryKeys,
                    s3Prefix,
                    eventTimestampMillis,
                    eventTimestampMillis);
            addToBuffer(new Record<>(pipelineEvent));
            eventCount++;
        }
        bytesProcessedSummary.record(bytes);

        flushBuffer(eventCount);
    }

    void handleUpdateEvent(com.github.shyiko.mysql.binlog.event.Event event) {
        final long bytes = event.toString().getBytes().length;
        bytesReceivedSummary.record(bytes);

        LOG.debug("Handling update event");
        final UpdateRowsEventData data = event.getData();
        if (!tableMetadataMap.containsKey(data.getTableId())) {
            return;
        }
        final TableMetadata tableMetadata = tableMetadataMap.get(data.getTableId());
        final String fullTableName = tableMetadata.getFullTableName();
        if (!isTableOfInterest(fullTableName)) {
            LOG.debug("The event is not from a table of interest");
            return;
        }
        final List<String> columnNames = tableMetadata.getColumnNames();
        final List<String> primaryKeys = tableMetadata.getPrimaryKeys();
        final long eventTimestampMillis = event.getHeader().getTimestamp();

        int eventCount = 0;
        for (Map.Entry<Serializable[], Serializable[]> updatedRow : data.getRows()) {
            // updatedRow contains data before update as key and data after update as value
            final Object[] rowData = updatedRow.getValue();

            final Map<String, Object> dataMap = new HashMap<>();
            for (int i = 0; i < rowData.length; i++) {
                dataMap.put(columnNames.get(i), rowData[i]);
            }

            final Event pipelineEvent = recordConverter.convert(
                    dataMap,
                    tableMetadata.getDatabaseName(),
                    tableMetadata.getTableName(),
                    event.getHeader().getEventType(),
                    OpenSearchBulkActions.INDEX,
                    primaryKeys,
                    s3Prefix,
                    eventTimestampMillis,
                    eventTimestampMillis);
            addToBuffer(new Record<>(pipelineEvent));
            eventCount++;
        }
        bytesProcessedSummary.record(bytes);

        flushBuffer(eventCount);
    }

    void handleDeleteEvent(com.github.shyiko.mysql.binlog.event.Event event) {
        final long bytes = event.toString().getBytes().length;
        bytesReceivedSummary.record(bytes);

        LOG.debug("Handling delete event");
        final DeleteRowsEventData data = event.getData();
        if (!tableMetadataMap.containsKey(data.getTableId())) {
            LOG.debug("Cannot find table metadata, the event is likely not from a table of interest or the table metadata was not read");
            return;
        }
        final TableMetadata tableMetadata = tableMetadataMap.get(data.getTableId());
        final String fullTableName = tableMetadata.getFullTableName();
        if (!isTableOfInterest(fullTableName)) {
            LOG.debug("The event is not from a table of interest");
            return;
        }
        final List<String> columnNames = tableMetadata.getColumnNames();
        final List<String> primaryKeys = tableMetadata.getPrimaryKeys();
        final long eventTimestampMillis = event.getHeader().getTimestamp();

        int eventCount = 0;
        for (Object[] rowDataArray : data.getRows()) {
            final Map<String, Object> rowDataMap = new HashMap<>();
            for (int i = 0; i < rowDataArray.length; i++) {
                rowDataMap.put(columnNames.get(i), rowDataArray[i]);
            }

            final Event pipelineEvent = recordConverter.convert(
                    rowDataMap,
                    tableMetadata.getDatabaseName(),
                    tableMetadata.getTableName(),
                    event.getHeader().getEventType(),
                    OpenSearchBulkActions.DELETE,
                    primaryKeys,
                    s3Prefix,
                    eventTimestampMillis,
                    eventTimestampMillis);
            addToBuffer(new Record<>(pipelineEvent));
            eventCount++;
        }
        bytesProcessedSummary.record(bytes);

        flushBuffer(eventCount);
    }

    private boolean isTableOfInterest(String tableName) {
        return new HashSet<>(tableNames).contains(tableName);
    }

    private void addToBuffer(final Record<Event> record) {
        try {
            bufferAccumulator.add(record);
        } catch (Exception e) {
            LOG.error("Failed to add event to buffer", e);
        }
    }

    private void flushBuffer(int eventCount) {
        try {
            bufferAccumulator.flush();
            changeEventSuccessCounter.increment(eventCount);
        } catch (Exception e) {
            LOG.error("Failed to flush buffer", e);
            changeEventErrorCounter.increment(eventCount);
        }
    }

    private void handleEventAndErrors(com.github.shyiko.mysql.binlog.event.Event event,
                                      Consumer<com.github.shyiko.mysql.binlog.event.Event> function) {
        try {
            function.accept(event);
        } catch (Exception e) {
            LOG.error("Failed to process change event", e);
        }
    }
}
