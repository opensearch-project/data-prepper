/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventMetadata;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.opensearch.dataprepper.plugins.source.rds.converter.StreamRecordConverter;
import org.opensearch.dataprepper.plugins.source.rds.model.BinlogCoordinate;
import org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
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
    private final BinaryLogClient binaryLogClient;
    private final BufferAccumulator<Record<Event>> bufferAccumulator;
    private final List<String> tableNames;
    private final String s3Prefix;
    private final boolean isAcknowledgmentsEnabled;
    private final PluginMetrics pluginMetrics;
    private final List<Event> pipelineEvents;
    private final StreamCheckpointManager streamCheckpointManager;
    private final Counter changeEventSuccessCounter;
    private final Counter changeEventErrorCounter;
    private final DistributionSummary bytesReceivedSummary;
    private final DistributionSummary bytesProcessedSummary;

    /**
     * currentBinlogCoordinate is the coordinate where next event will start
     */
    private BinlogCoordinate currentBinlogCoordinate;

    public BinlogEventListener(final Buffer<Record<Event>> buffer,
                               final RdsSourceConfig sourceConfig,
                               final PluginMetrics pluginMetrics,
                               final BinaryLogClient binaryLogClient,
                               final StreamCheckpointer streamCheckpointer,
                               final AcknowledgementSetManager acknowledgementSetManager) {
        this.binaryLogClient = binaryLogClient;
        tableMetadataMap = new HashMap<>();
        recordConverter = new StreamRecordConverter(sourceConfig.getStream().getPartitionCount());
        bufferAccumulator = BufferAccumulator.create(buffer, DEFAULT_BUFFER_BATCH_SIZE, BUFFER_TIMEOUT);
        s3Prefix = sourceConfig.getS3Prefix();
        tableNames = sourceConfig.getTableNames();
        isAcknowledgmentsEnabled = sourceConfig.isAcknowledgmentsEnabled();
        this.pluginMetrics = pluginMetrics;
        pipelineEvents = new ArrayList<>();

        this.streamCheckpointManager = new StreamCheckpointManager(
                streamCheckpointer, sourceConfig.isAcknowledgmentsEnabled(),
                acknowledgementSetManager, this::stopClient, sourceConfig.getStreamAcknowledgmentTimeout());
        streamCheckpointManager.start();

        changeEventSuccessCounter = pluginMetrics.counter(CHANGE_EVENTS_PROCESSED_COUNT);
        changeEventErrorCounter = pluginMetrics.counter(CHANGE_EVENTS_PROCESSING_ERROR_COUNT);
        bytesReceivedSummary = pluginMetrics.summary(BYTES_RECEIVED);
        bytesProcessedSummary = pluginMetrics.summary(BYTES_PROCESSED);
    }

    @Override
    public void onEvent(com.github.shyiko.mysql.binlog.event.Event event) {
        final EventType eventType = event.getHeader().getEventType();

        switch (eventType) {
            case ROTATE:
                handleEventAndErrors(event, this::handleRotateEvent);
                break;
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

    public void stopClient() {
        try {
            binaryLogClient.disconnect();
            LOG.info("Binary log client disconnected.");
        } catch (Exception e) {
            LOG.error("Binary log client failed to disconnect.", e);
        }
    }

    void handleRotateEvent(com.github.shyiko.mysql.binlog.event.Event event) {
        final RotateEventData data = event.getData();
        currentBinlogCoordinate = new BinlogCoordinate(data.getBinlogFilename(), data.getBinlogPosition());

        // Trigger a checkpoint update for this rotate when there're no row mutation events being processed
        if (streamCheckpointManager.getChangeEventStatuses().isEmpty()) {
            ChangeEventStatus changeEventStatus = streamCheckpointManager.saveChangeEventsStatus(currentBinlogCoordinate);
            if (isAcknowledgmentsEnabled) {
                changeEventStatus.setAcknowledgmentStatus(ChangeEventStatus.AcknowledgmentStatus.POSITIVE_ACK);
            }
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
        LOG.debug("Handling insert event");
        final WriteRowsEventData data = event.getData();
        handleRowChangeEvent(event, data.getTableId(), data.getRows(), OpenSearchBulkActions.INDEX);
    }

    void handleUpdateEvent(com.github.shyiko.mysql.binlog.event.Event event) {
        LOG.debug("Handling update event");
        final UpdateRowsEventData data = event.getData();

        // updatedRow contains data before update as key and data after update as value
        final List<Serializable[]> rows = data.getRows().stream()
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());

        handleRowChangeEvent(event, data.getTableId(), rows, OpenSearchBulkActions.INDEX);
    }

    void handleDeleteEvent(com.github.shyiko.mysql.binlog.event.Event event) {
        LOG.debug("Handling delete event");
        final DeleteRowsEventData data = event.getData();

        handleRowChangeEvent(event, data.getTableId(), data.getRows(), OpenSearchBulkActions.DELETE);
    }

    private void handleRowChangeEvent(com.github.shyiko.mysql.binlog.event.Event event,
                              long tableId,
                              List<Serializable[]> rows,
                              OpenSearchBulkActions bulkAction) {

        // Update binlog coordinate after it's first assigned in rotate event handler
        if (currentBinlogCoordinate != null) {
            final EventHeaderV4 eventHeader = event.getHeader();
            currentBinlogCoordinate = new BinlogCoordinate(currentBinlogCoordinate.getBinlogFilename(), eventHeader.getNextPosition());
            LOG.debug("Current binlog coordinate after receiving a row change event: " + currentBinlogCoordinate);
        }

        AcknowledgementSet acknowledgementSet = null;
        if (isAcknowledgmentsEnabled) {
            acknowledgementSet = streamCheckpointManager.createAcknowledgmentSet(currentBinlogCoordinate);
        }

        final long bytes = event.toString().getBytes().length;
        bytesReceivedSummary.record(bytes);

        if (!tableMetadataMap.containsKey(tableId)) {
            LOG.debug("Cannot find table metadata, the event is likely not from a table of interest or the table metadata was not read");
            return;
        }
        final TableMetadata tableMetadata = tableMetadataMap.get(tableId);
        final String fullTableName = tableMetadata.getFullTableName();
        if (!isTableOfInterest(fullTableName)) {
            LOG.debug("The event is not from a table of interest");
            return;
        }
        final List<String> columnNames = tableMetadata.getColumnNames();
        final List<String> primaryKeys = tableMetadata.getPrimaryKeys();
        final long eventTimestampMillis = event.getHeader().getTimestamp();

        for (Object[] rowDataArray : rows) {
            final Map<String, Object> rowDataMap = new HashMap<>();
            for (int i = 0; i < rowDataArray.length; i++) {
                rowDataMap.put(columnNames.get(i), rowDataArray[i]);
            }

            final Event pipelineEvent = recordConverter.convert(
                    rowDataMap,
                    tableMetadata.getDatabaseName(),
                    tableMetadata.getTableName(),
                    event.getHeader().getEventType(),
                    bulkAction,
                    primaryKeys,
                    s3Prefix,
                    eventTimestampMillis,
                    eventTimestampMillis);
            pipelineEvents.add(pipelineEvent);
        }

        writeToBuffer(acknowledgementSet);
        bytesProcessedSummary.record(bytes);

        if (isAcknowledgmentsEnabled) {
            acknowledgementSet.complete();
        } else {
            streamCheckpointManager.saveChangeEventsStatus(currentBinlogCoordinate);
        }
    }

    private boolean isTableOfInterest(String tableName) {
        return new HashSet<>(tableNames).contains(tableName);
    }

    private void writeToBuffer(AcknowledgementSet acknowledgementSet) {
        for (Event pipelineEvent : pipelineEvents) {
            addToBufferAccumulator(new Record<>(pipelineEvent));
            if (acknowledgementSet != null) {
                acknowledgementSet.add(pipelineEvent);
            }
        }

        flushBufferAccumulator(pipelineEvents.size());
        pipelineEvents.clear();
    }

    private void addToBufferAccumulator(final Record<Event> record) {
        try {
            bufferAccumulator.add(record);
        } catch (Exception e) {
            LOG.error("Failed to add event to buffer", e);
        }
    }

    private void flushBufferAccumulator(int eventCount) {
        try {
            bufferAccumulator.flush();
            changeEventSuccessCounter.increment(eventCount);
        } catch (Exception e) {
            // this will only happen if writing to buffer gets interrupted from shutdown,
            // otherwise bufferAccumulator will keep retrying with backoff
            LOG.error("Failed to flush buffer", e);
            changeEventErrorCounter.increment(eventCount);
        }
    }

    private void handleEventAndErrors(com.github.shyiko.mysql.binlog.event.Event event,
                                      Consumer<com.github.shyiko.mysql.binlog.event.Event> function) {
        try {
            function.accept(event);
        } catch (Exception e) {
            LOG.error("Failed to process change event of type {}", event.getHeader().getEventType(), e);
        }
    }
}
