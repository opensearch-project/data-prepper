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
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.common.concurrent.BackgroundThreadFactory;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.opensearch.dataprepper.plugins.source.rds.converter.StreamRecordConverter;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.rds.model.BinlogCoordinate;
import org.opensearch.dataprepper.plugins.source.rds.model.ParentTable;
import org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata;
import org.opensearch.dataprepper.plugins.source.rds.resync.CascadingActionDetector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class BinlogEventListener implements BinaryLogClient.EventListener {

    private static final Logger LOG = LoggerFactory.getLogger(BinlogEventListener.class);

    static final Duration BUFFER_TIMEOUT = Duration.ofSeconds(60);
    static final int DEFAULT_BUFFER_BATCH_SIZE = 1_000;
    static final String DATA_PREPPER_EVENT_TYPE = "event";
    static final String CHANGE_EVENTS_PROCESSED_COUNT = "changeEventsProcessed";
    static final String CHANGE_EVENTS_PROCESSING_ERROR_COUNT = "changeEventsProcessingErrors";
    static final String BYTES_RECEIVED = "bytesReceived";
    static final String BYTES_PROCESSED = "bytesProcessed";
    static final String REPLICATION_LOG_EVENT_PROCESSING_TIME = "replicationLogEntryProcessingTime";

    /**
     * TableId to TableMetadata mapping
     */
    private final Map<Long, TableMetadata> tableMetadataMap;

    /**
     * TableName to ParentTable mapping. Only parent tables that have cascading update/delete actions defined
     * (CASCADE, SET_NULL, SET_DEFAULT) are included in this map.
     */
    private final Map<String, ParentTable> parentTableMap;

    private final StreamPartition streamPartition;
    private final StreamRecordConverter recordConverter;
    private final BinaryLogClient binaryLogClient;
    private final Buffer<Record<Event>> buffer;
    private final List<String> tableNames;
    private final String s3Prefix;
    private final boolean isAcknowledgmentsEnabled;
    private final PluginMetrics pluginMetrics;
    private final List<Event> pipelineEvents;
    private final StreamCheckpointManager streamCheckpointManager;
    private final ExecutorService binlogEventExecutorService;
    private final CascadingActionDetector cascadeActionDetector;

    private final Counter changeEventSuccessCounter;
    private final Counter changeEventErrorCounter;
    private final DistributionSummary bytesReceivedSummary;
    private final DistributionSummary bytesProcessedSummary;
    private final Timer eventProcessingTimer;


    /**
     * currentBinlogCoordinate is the coordinate where next event will start
     */
    private BinlogCoordinate currentBinlogCoordinate;

    public BinlogEventListener(final StreamPartition streamPartition,
                               final Buffer<Record<Event>> buffer,
                               final RdsSourceConfig sourceConfig,
                               final String s3Prefix,
                               final PluginMetrics pluginMetrics,
                               final BinaryLogClient binaryLogClient,
                               final StreamCheckpointer streamCheckpointer,
                               final AcknowledgementSetManager acknowledgementSetManager,
                               final CascadingActionDetector cascadeActionDetector) {
        this.streamPartition = streamPartition;
        this.buffer = buffer;
        this.binaryLogClient = binaryLogClient;
        tableMetadataMap = new HashMap<>();
        recordConverter = new StreamRecordConverter(s3Prefix, sourceConfig.getPartitionCount());
        this.s3Prefix = s3Prefix;
        tableNames = sourceConfig.getTableNames();
        isAcknowledgmentsEnabled = sourceConfig.isAcknowledgmentsEnabled();
        this.pluginMetrics = pluginMetrics;
        pipelineEvents = new ArrayList<>();
        binlogEventExecutorService = Executors.newFixedThreadPool(
                sourceConfig.getStream().getNumWorkers(), BackgroundThreadFactory.defaultExecutorThreadFactory("rds-source-binlog-processor"));

        this.streamCheckpointManager = new StreamCheckpointManager(
                streamCheckpointer, sourceConfig.isAcknowledgmentsEnabled(),
                acknowledgementSetManager, this::stopClient, sourceConfig.getStreamAcknowledgmentTimeout());
        streamCheckpointManager.start();

        this.cascadeActionDetector = cascadeActionDetector;
        parentTableMap = cascadeActionDetector.getParentTableMap(streamPartition);

        changeEventSuccessCounter = pluginMetrics.counter(CHANGE_EVENTS_PROCESSED_COUNT);
        changeEventErrorCounter = pluginMetrics.counter(CHANGE_EVENTS_PROCESSING_ERROR_COUNT);
        bytesReceivedSummary = pluginMetrics.summary(BYTES_RECEIVED);
        bytesProcessedSummary = pluginMetrics.summary(BYTES_PROCESSED);
        eventProcessingTimer = pluginMetrics.timer(REPLICATION_LOG_EVENT_PROCESSING_TIME);
    }

    public static BinlogEventListener create(final StreamPartition streamPartition,
                                             final Buffer<Record<Event>> buffer,
                                             final RdsSourceConfig sourceConfig,
                                             final String s3Prefix,
                                             final PluginMetrics pluginMetrics,
                                             final BinaryLogClient binaryLogClient,
                                             final StreamCheckpointer streamCheckpointer,
                                             final AcknowledgementSetManager acknowledgementSetManager,
                                             final CascadingActionDetector cascadeActionDetector) {
        return new BinlogEventListener(streamPartition, buffer, sourceConfig, s3Prefix, pluginMetrics, binaryLogClient, streamCheckpointer, acknowledgementSetManager, cascadeActionDetector);
    }

    @Override
    public void onEvent(com.github.shyiko.mysql.binlog.event.Event event) {
        final EventType eventType = event.getHeader().getEventType();

        switch (eventType) {
            case ROTATE:
                processEvent(event, this::handleRotateEvent);
                break;
            case TABLE_MAP:
                processEvent(event, this::handleTableMapEvent);
                break;
            case WRITE_ROWS:
            case EXT_WRITE_ROWS:
                processEvent(event, this::handleInsertEvent);
                break;
            case UPDATE_ROWS:
            case EXT_UPDATE_ROWS:
                processEvent(event, this::handleUpdateEvent);
                break;
            case DELETE_ROWS:
            case EXT_DELETE_ROWS:
                processEvent(event, this::handleDeleteEvent);
                break;
        }
    }

    public void stopClient() {
        try {
            binaryLogClient.disconnect();
            binaryLogClient.unregisterEventListener(this);
            binlogEventExecutorService.shutdownNow();
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

        if (!isValidTableId(data.getTableId())) {
            return;
        }

        handleRowChangeEvent(event, data.getTableId(), data.getRows(), OpenSearchBulkActions.INDEX);
    }

    void handleUpdateEvent(com.github.shyiko.mysql.binlog.event.Event event) {
        LOG.debug("Handling update event");
        final UpdateRowsEventData data = event.getData();

        if (!isValidTableId(data.getTableId())) {
            return;
        }

        // Check if a cascade action is involved
        cascadeActionDetector.detectCascadingUpdates(event, parentTableMap, tableMetadataMap.get(data.getTableId()));

        // updatedRow contains data before update as key and data after update as value
        final List<Serializable[]> rows = data.getRows().stream()
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());

        handleRowChangeEvent(event, data.getTableId(), rows, OpenSearchBulkActions.INDEX);
    }

    void handleDeleteEvent(com.github.shyiko.mysql.binlog.event.Event event) {
        LOG.debug("Handling delete event");
        final DeleteRowsEventData data = event.getData();

        if (!isValidTableId(data.getTableId())) {
            return;
        }

        // Check if a cascade action is involved
        cascadeActionDetector.detectCascadingDeletes(event, parentTableMap, tableMetadataMap.get(data.getTableId()));

        handleRowChangeEvent(event, data.getTableId(), data.getRows(), OpenSearchBulkActions.DELETE);
    }

    private boolean isValidTableId(long tableId) {
        if (!tableMetadataMap.containsKey(tableId)) {
            LOG.debug("Cannot find table metadata, the event is likely not from a table of interest or the table metadata was not read");
            return false;
        }

        if (!isTableOfInterest(tableMetadataMap.get(tableId).getFullTableName())) {
            LOG.debug("The event is not from a table of interest");
            return false;
        }

        return true;
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

        final TableMetadata tableMetadata = tableMetadataMap.get(tableId);
        final List<String> columnNames = tableMetadata.getColumnNames();
        final List<String> primaryKeys = tableMetadata.getPrimaryKeys();
        final long eventTimestampMillis = event.getHeader().getTimestamp();

        final BufferAccumulator<Record<Event>> bufferAccumulator = BufferAccumulator.create(buffer, DEFAULT_BUFFER_BATCH_SIZE, BUFFER_TIMEOUT);

        for (Object[] rowDataArray : rows) {
            final Map<String, Object> rowDataMap = new HashMap<>();
            for (int i = 0; i < rowDataArray.length; i++) {
                rowDataMap.put(columnNames.get(i), rowDataArray[i]);
            }

            final Event dataPrepperEvent = JacksonEvent.builder()
                    .withEventType(DATA_PREPPER_EVENT_TYPE)
                    .withData(rowDataMap)
                    .build();

            final Event pipelineEvent = recordConverter.convert(
                    dataPrepperEvent,
                    tableMetadata.getDatabaseName(),
                    tableMetadata.getTableName(),
                    bulkAction,
                    primaryKeys,
                    eventTimestampMillis,
                    eventTimestampMillis,
                    event.getHeader().getEventType());
            pipelineEvents.add(pipelineEvent);
        }

        writeToBuffer(bufferAccumulator, acknowledgementSet);
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

    private void writeToBuffer(BufferAccumulator<Record<Event>> bufferAccumulator, AcknowledgementSet acknowledgementSet) {
        for (Event pipelineEvent : pipelineEvents) {
            addToBufferAccumulator(bufferAccumulator, new Record<>(pipelineEvent));
            if (acknowledgementSet != null) {
                acknowledgementSet.add(pipelineEvent);
            }
        }

        flushBufferAccumulator(bufferAccumulator, pipelineEvents.size());
        pipelineEvents.clear();
    }

    private void addToBufferAccumulator(final BufferAccumulator<Record<Event>> bufferAccumulator, final Record<Event> record) {
        try {
            bufferAccumulator.add(record);
        } catch (Exception e) {
            LOG.error("Failed to add event to buffer", e);
        }
    }

    private void flushBufferAccumulator(BufferAccumulator<Record<Event>> bufferAccumulator, int eventCount) {
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

    private void processEvent(com.github.shyiko.mysql.binlog.event.Event event, Consumer<com.github.shyiko.mysql.binlog.event.Event> function) {
        binlogEventExecutorService.submit(() -> handleEventAndErrors(event, function));
    }

    private void handleEventAndErrors(com.github.shyiko.mysql.binlog.event.Event event,
                                      Consumer<com.github.shyiko.mysql.binlog.event.Event> function) {
        try {
            eventProcessingTimer.record(() -> function.accept(event));
        } catch (Exception e) {
            LOG.error("Failed to process change event of type {}", event.getHeader().getEventType(), e);
        }
    }
}
