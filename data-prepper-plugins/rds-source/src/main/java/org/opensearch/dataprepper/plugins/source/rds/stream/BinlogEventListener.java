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
import org.opensearch.dataprepper.plugins.source.rds.datatype.mysql.MySQLDataType;
import org.opensearch.dataprepper.plugins.source.rds.datatype.mysql.MySQLDataTypeHelper;
import org.opensearch.dataprepper.plugins.source.rds.model.BinlogCoordinate;
import org.opensearch.dataprepper.plugins.source.rds.model.DbTableMetadata;
import org.opensearch.dataprepper.plugins.source.rds.model.ParentTable;
import org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata;
import org.opensearch.dataprepper.plugins.source.rds.resync.CascadingActionDetector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.NOISY;

public class BinlogEventListener implements BinaryLogClient.EventListener {

    private static final Logger LOG = LoggerFactory.getLogger(BinlogEventListener.class);

    static final int DEFAULT_NUM_WORKERS = 1;
    static final Duration BUFFER_TIMEOUT = Duration.ofSeconds(60);
    static final int DEFAULT_BUFFER_BATCH_SIZE = 1_000;
    static final String DATA_PREPPER_EVENT_TYPE = "event";
    static final String CHANGE_EVENTS_PROCESSED_COUNT = "changeEventsProcessed";
    static final String CHANGE_EVENTS_PROCESSING_ERROR_COUNT = "changeEventsProcessingErrors";
    static final String BYTES_RECEIVED = "bytesReceived";
    static final String BYTES_PROCESSED = "bytesProcessed";
    static final String REPLICATION_LOG_EVENT_PROCESSING_TIME = "replicationLogEntryProcessingTime";
    static final String REPLICATION_LOG_PROCESSING_ERROR_COUNT = "replicationLogEntryProcessingErrors";
    static final String SEPARATOR = ".";

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
    private final Set<String> tableNames;
    private final String s3Prefix;
    private final boolean isAcknowledgmentsEnabled;
    private final PluginMetrics pluginMetrics;
    private final List<Event> pipelineEvents;
    private final StreamCheckpointManager streamCheckpointManager;
    private final DbTableMetadata dbTableMetadata;
    private final ExecutorService binlogEventExecutorService;
    private final CascadingActionDetector cascadeActionDetector;

    private final Counter changeEventSuccessCounter;
    private final Counter changeEventErrorCounter;
    private final DistributionSummary bytesReceivedSummary;
    private final DistributionSummary bytesProcessedSummary;
    private final Timer eventProcessingTimer;
    private final Counter eventProcessingErrorCounter;

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
                               final DbTableMetadata dbTableMetadata,
                               final CascadingActionDetector cascadeActionDetector) {
        this.streamPartition = streamPartition;
        this.buffer = buffer;
        this.binaryLogClient = binaryLogClient;
        tableMetadataMap = new HashMap<>();
        recordConverter = new StreamRecordConverter(s3Prefix, sourceConfig.getPartitionCount());
        this.s3Prefix = s3Prefix;
        tableNames = dbTableMetadata.getTableColumnDataTypeMap().keySet();
        isAcknowledgmentsEnabled = sourceConfig.isAcknowledgmentsEnabled();
        this.pluginMetrics = pluginMetrics;
        pipelineEvents = new ArrayList<>();
        binlogEventExecutorService = Executors.newFixedThreadPool(
                DEFAULT_NUM_WORKERS, BackgroundThreadFactory.defaultExecutorThreadFactory("rds-source-binlog-processor"));

        this.dbTableMetadata = dbTableMetadata;
        this.streamCheckpointManager = new StreamCheckpointManager(
                streamCheckpointer, sourceConfig.isAcknowledgmentsEnabled(),
                acknowledgementSetManager, this::stopClient, sourceConfig.getStreamAcknowledgmentTimeout(),
                sourceConfig.getEngine(), pluginMetrics);
        streamCheckpointManager.start();

        this.cascadeActionDetector = cascadeActionDetector;
        parentTableMap = cascadeActionDetector.getParentTableMap(streamPartition);

        changeEventSuccessCounter = pluginMetrics.counter(CHANGE_EVENTS_PROCESSED_COUNT);
        changeEventErrorCounter = pluginMetrics.counter(CHANGE_EVENTS_PROCESSING_ERROR_COUNT);
        bytesReceivedSummary = pluginMetrics.summary(BYTES_RECEIVED);
        bytesProcessedSummary = pluginMetrics.summary(BYTES_PROCESSED);
        eventProcessingTimer = pluginMetrics.timer(REPLICATION_LOG_EVENT_PROCESSING_TIME);
        eventProcessingErrorCounter = pluginMetrics.counter(REPLICATION_LOG_PROCESSING_ERROR_COUNT);
    }

    public static BinlogEventListener create(final StreamPartition streamPartition,
                                             final Buffer<Record<Event>> buffer,
                                             final RdsSourceConfig sourceConfig,
                                             final String s3Prefix,
                                             final PluginMetrics pluginMetrics,
                                             final BinaryLogClient binaryLogClient,
                                             final StreamCheckpointer streamCheckpointer,
                                             final AcknowledgementSetManager acknowledgementSetManager,
                                             final DbTableMetadata dbTableMetadata,
                                             final CascadingActionDetector cascadeActionDetector) {
        return new BinlogEventListener(streamPartition, buffer, sourceConfig, s3Prefix, pluginMetrics, binaryLogClient, 
                                       streamCheckpointer, acknowledgementSetManager, dbTableMetadata, cascadeActionDetector);
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
            ChangeEventStatus changeEventStatus = streamCheckpointManager.saveChangeEventsStatus(currentBinlogCoordinate, 0);
            if (isAcknowledgmentsEnabled) {
                changeEventStatus.setAcknowledgmentStatus(ChangeEventStatus.AcknowledgmentStatus.POSITIVE_ACK);
            }
        }
    }

    void handleTableMapEvent(com.github.shyiko.mysql.binlog.event.Event event) {
        final TableMapEventData eventData = event.getData();
        final String databaseName = eventData.getDatabase();
        final String tableName = eventData.getTable();
        final String fullTableName = databaseName + SEPARATOR + tableName;

        if (!isTableOfInterest(fullTableName)) {
            return;
        }

        final TableMapEventMetadata tableMapEventMetadata = eventData.getEventMetadata();
        final List<String> columnNames = tableMapEventMetadata.getColumnNames();
        final List<String> primaryKeys = tableMapEventMetadata.getSimplePrimaryKeys().stream()
                .map(columnNames::get)
                .collect(Collectors.toList());
        final TableMetadata tableMetadata = TableMetadata.builder()
                .withTableName(tableName)
                .withDatabaseName(databaseName)
                .withColumnNames(columnNames)
                .withPrimaryKeys(primaryKeys)
                .withSetStrValues(getSetStrValues(eventData))
                .withEnumStrValues(getEnumStrValues(eventData))
                .build();
        tableMetadataMap.put(eventData.getTableId(), tableMetadata);
    }

    private Map<String, String[]> getSetStrValues(final TableMapEventData eventData) {
        return getStrValuesMap(eventData, MySQLDataType.SET);
    }

    private Map<String, String[]> getEnumStrValues(final TableMapEventData eventData) {
        return getStrValuesMap(eventData, MySQLDataType.ENUM);
    }

    private Map<String, String[]> getStrValuesMap(final TableMapEventData eventData, final MySQLDataType columnType) {
        Map<String, String[]> strValuesMap = new HashMap<>();
        List<String> columnNames = eventData.getEventMetadata().getColumnNames();
        List<String[]> strValues = getStrValues(eventData, columnType);

        final Map<String, String> tbMetadata = dbTableMetadata.getTableColumnDataTypeMap()
                .get(eventData.getDatabase() + SEPARATOR + eventData.getTable());

        for (int i = 0, j=0; i < columnNames.size(); i++) {
            final String dataType = tbMetadata.get(columnNames.get(i));
            if (MySQLDataType.byDataType(dataType) == columnType) {
                strValuesMap.put(columnNames.get(i), strValues.get(j++));
            }
        }

        return strValuesMap;
    }

    private List<String[]> getStrValues(final TableMapEventData eventData, final MySQLDataType columnType) {
        if (columnType == MySQLDataType.ENUM) {
            return eventData.getEventMetadata().getEnumStrValues();
        } else if (columnType == MySQLDataType.SET) {
            return eventData.getEventMetadata().getSetStrValues();
        } else {
            return Collections.emptyList();
        }
    }

    void handleInsertEvent(com.github.shyiko.mysql.binlog.event.Event event) {
        LOG.debug("Handling insert event");
        final WriteRowsEventData data = event.getData();

        if (!isValidTableId(data.getTableId())) {
            return;
        }

        handleRowChangeEvent(event, data.getTableId(), data.getRows(), Collections.nCopies(data.getRows().size(), OpenSearchBulkActions.INDEX));
    }

    void handleUpdateEvent(com.github.shyiko.mysql.binlog.event.Event event) {
        LOG.debug("Handling update event");
        final UpdateRowsEventData data = event.getData();

        if (!isValidTableId(data.getTableId())) {
            return;
        }

        // Check if a cascade action is involved
        cascadeActionDetector.detectCascadingUpdates(event, parentTableMap, tableMetadataMap.get(data.getTableId()));

        final TableMetadata tableMetadata = tableMetadataMap.get(data.getTableId());
        final List<OpenSearchBulkActions> bulkActions = new ArrayList<>();
        final List<Serializable[]> rows = new ArrayList<>();
        for (int rowNum = 0; rowNum < data.getRows().size(); rowNum++) {
            // `row` contains data before update as key and data after update as value
            Map.Entry<Serializable[], Serializable[]> row = data.getRows().get(rowNum);

            for (int i = 0; i < row.getKey().length; i++) {
                if (tableMetadata.getPrimaryKeys().contains(tableMetadata.getColumnNames().get(i)) &&
                        !row.getKey()[i].equals(row.getValue()[i])) {
                    LOG.debug("Primary keys were updated");
                    // add delete event for the old row data
                    rows.add(row.getKey());
                    bulkActions.add(OpenSearchBulkActions.DELETE);
                    break;
                }
            }
            // add index event for the new row data
            rows.add(row.getValue());
            bulkActions.add(OpenSearchBulkActions.INDEX);
        }

        handleRowChangeEvent(event, data.getTableId(), rows, bulkActions);
    }

    void handleDeleteEvent(com.github.shyiko.mysql.binlog.event.Event event) {
        LOG.debug("Handling delete event");
        final DeleteRowsEventData data = event.getData();

        if (!isValidTableId(data.getTableId())) {
            return;
        }

        // Check if a cascade action is involved
        cascadeActionDetector.detectCascadingDeletes(event, parentTableMap, tableMetadataMap.get(data.getTableId()));

        handleRowChangeEvent(event, data.getTableId(), data.getRows(), Collections.nCopies(data.getRows().size(), OpenSearchBulkActions.DELETE));
    }

    // Visible For Testing
    boolean isValidTableId(long tableId) {
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

    // Visible For Testing
    void handleRowChangeEvent(com.github.shyiko.mysql.binlog.event.Event event,
                              long tableId,
                              List<Serializable[]> rows,
                              List<OpenSearchBulkActions> bulkActions) {

        // Update binlog coordinate after it's first assigned in rotate event handler
        if (currentBinlogCoordinate != null) {
            final EventHeaderV4 eventHeader = event.getHeader();
            currentBinlogCoordinate = new BinlogCoordinate(currentBinlogCoordinate.getBinlogFilename(), eventHeader.getNextPosition());
            LOG.debug("Current binlog coordinate after receiving a row change event: " + currentBinlogCoordinate);
        }

        final long recordCount = rows.size();
        AcknowledgementSet acknowledgementSet = null;
        if (isAcknowledgmentsEnabled) {
            acknowledgementSet = streamCheckpointManager.createAcknowledgmentSet(currentBinlogCoordinate, recordCount);
        }

        final long bytes = event.toString().getBytes().length;
        bytesReceivedSummary.record(bytes);

        final TableMetadata tableMetadata = tableMetadataMap.get(tableId);
        final List<String> columnNames = tableMetadata.getColumnNames();
        final List<String> primaryKeys = tableMetadata.getPrimaryKeys();
        final long eventTimestampMillis = event.getHeader().getTimestamp();

        final BufferAccumulator<Record<Event>> bufferAccumulator = BufferAccumulator.create(buffer, DEFAULT_BUFFER_BATCH_SIZE, BUFFER_TIMEOUT);

        for (int rowNum = 0; rowNum < rows.size(); rowNum++) {
            final Object[] rowDataArray = rows.get(rowNum);
            final OpenSearchBulkActions bulkAction = bulkActions.get(rowNum);

            final Map<String, Object> rowDataMap = new HashMap<>();
            for (int i = 0; i < rowDataArray.length; i++) {
                final Map<String, String> tbColumnDatatypeMap = dbTableMetadata.getTableColumnDataTypeMap().get(tableMetadata.getFullTableName());
                final String columnDataType = tbColumnDatatypeMap.get(columnNames.get(i));
                final Object data =  MySQLDataTypeHelper.getDataByColumnType(MySQLDataType.byDataType(columnDataType), columnNames.get(i),
                        rowDataArray[i], tableMetadata);
                rowDataMap.put(columnNames.get(i), data);
            }

            final Event dataPrepperEvent = JacksonEvent.builder()
                    .withEventType(DATA_PREPPER_EVENT_TYPE)
                    .withData(rowDataMap)
                    .build();

            final Event pipelineEvent = recordConverter.convert(
                    dataPrepperEvent,
                    tableMetadata.getDatabaseName(),
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
            streamCheckpointManager.saveChangeEventsStatus(currentBinlogCoordinate, recordCount);
        }
    }

    private boolean isTableOfInterest(String tableName) {
        return tableNames.contains(tableName);
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
            LOG.error(NOISY, "Failed to process change event of type {}", event.getHeader().getEventType(), e);
            eventProcessingErrorCounter.increment();
        }
    }
}
