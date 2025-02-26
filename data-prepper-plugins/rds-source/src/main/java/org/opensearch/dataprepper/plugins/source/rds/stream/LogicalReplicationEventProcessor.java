/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.rds.stream;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
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
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.StreamProgressState;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.ColumnType;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataType;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.PostgresDataTypeHelper;
import org.opensearch.dataprepper.plugins.source.rds.model.MessageType;
import org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata;
import org.postgresql.replication.LogSequenceNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public class LogicalReplicationEventProcessor {

    enum TupleDataType {
        NEW('N'),
        KEY('K'),
        OLD('O');

        private final char value;

        TupleDataType(char value) {
            this.value = value;
        }

        public char getValue() {
            return value;
        }

        public static TupleDataType fromValue(char value) {
            for (TupleDataType type : TupleDataType.values()) {
                if (type.getValue() == value) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Invalid TupleDataType value: " + value);
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(LogicalReplicationEventProcessor.class);

    static final Duration BUFFER_TIMEOUT = Duration.ofSeconds(60);
    static final int DEFAULT_BUFFER_BATCH_SIZE = 1_000;
    static final int NUM_OF_RETRIES = 3;
    static final int BACKOFF_IN_MILLIS = 500;
    static final String DOT_DELIMITER_REGEX = "\\.";
    static final String CHANGE_EVENTS_PROCESSED_COUNT = "changeEventsProcessed";
    static final String CHANGE_EVENTS_PROCESSING_ERROR_COUNT = "changeEventsProcessingErrors";
    static final String BYTES_RECEIVED = "bytesReceived";
    static final String BYTES_PROCESSED = "bytesProcessed";
    static final String REPLICATION_LOG_EVENT_PROCESSING_TIME = "replicationLogEntryProcessingTime";
    static final String REPLICATION_LOG_PROCESSING_ERROR_COUNT = "replicationLogEntryProcessingErrors";

    private final StreamPartition streamPartition;
    private final RdsSourceConfig sourceConfig;
    private final StreamRecordConverter recordConverter;
    private final Buffer<Record<Event>> buffer;
    private final BufferAccumulator<Record<Event>> bufferAccumulator;
    private final List<Event> pipelineEvents;
    private final PluginMetrics pluginMetrics;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final LogicalReplicationClient logicalReplicationClient;
    private final StreamCheckpointer streamCheckpointer;
    private final StreamCheckpointManager streamCheckpointManager;

    private final Counter changeEventSuccessCounter;
    private final Counter changeEventErrorCounter;
    private final DistributionSummary bytesReceivedSummary;
    private final DistributionSummary bytesProcessedSummary;
    private final Timer eventProcessingTimer;
    private final Counter eventProcessingErrorCounter;

    private long currentLsn;
    private long currentEventTimestamp;
    private long bytesReceived;

    private Map<Long, TableMetadata> tableMetadataMap;

    public LogicalReplicationEventProcessor(final StreamPartition streamPartition,
                                            final RdsSourceConfig sourceConfig,
                                            final Buffer<Record<Event>> buffer,
                                            final String s3Prefix,
                                            final PluginMetrics pluginMetrics,
                                            final LogicalReplicationClient logicalReplicationClient,
                                            final StreamCheckpointer streamCheckpointer,
                                            final AcknowledgementSetManager acknowledgementSetManager) {
        this.streamPartition = streamPartition;
        this.sourceConfig = sourceConfig;
        recordConverter = new StreamRecordConverter(s3Prefix, sourceConfig.getPartitionCount());
        this.buffer = buffer;
        bufferAccumulator = BufferAccumulator.create(buffer, DEFAULT_BUFFER_BATCH_SIZE, BUFFER_TIMEOUT);
        this.pluginMetrics = pluginMetrics;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.logicalReplicationClient = logicalReplicationClient;
        this.streamCheckpointer = streamCheckpointer;
        streamCheckpointManager = new StreamCheckpointManager(
                streamCheckpointer, sourceConfig.isAcknowledgmentsEnabled(),
                acknowledgementSetManager, this::stopClient, sourceConfig.getStreamAcknowledgmentTimeout(),
                sourceConfig.getEngine(), pluginMetrics);
        streamCheckpointManager.start();

        tableMetadataMap = new HashMap<>();
        pipelineEvents = new ArrayList<>();

        changeEventSuccessCounter = pluginMetrics.counter(CHANGE_EVENTS_PROCESSED_COUNT);
        changeEventErrorCounter = pluginMetrics.counter(CHANGE_EVENTS_PROCESSING_ERROR_COUNT);
        bytesReceivedSummary = pluginMetrics.summary(BYTES_RECEIVED);
        bytesProcessedSummary = pluginMetrics.summary(BYTES_PROCESSED);
        eventProcessingTimer = pluginMetrics.timer(REPLICATION_LOG_EVENT_PROCESSING_TIME);
        eventProcessingErrorCounter = pluginMetrics.counter(REPLICATION_LOG_PROCESSING_ERROR_COUNT);
    }

    public static LogicalReplicationEventProcessor create(final StreamPartition streamPartition,
                                                          final RdsSourceConfig sourceConfig,
                                                          final Buffer<Record<Event>> buffer,
                                                          final String s3Prefix,
                                                          final PluginMetrics pluginMetrics,
                                                          final LogicalReplicationClient logicalReplicationClient,
                                                          final StreamCheckpointer streamCheckpointer,
                                                          final AcknowledgementSetManager acknowledgementSetManager) {
        return new LogicalReplicationEventProcessor(streamPartition, sourceConfig, buffer, s3Prefix, pluginMetrics,
                logicalReplicationClient, streamCheckpointer, acknowledgementSetManager);
    }

    public void process(ByteBuffer msg) {
        // Message processing logic:
        // If it's a BEGIN, note its LSN
        // If it's a RELATION, update table metadata map
        // If it's INSERT/UPDATE/DELETE, prepare events
        // If it's a COMMIT, convert all prepared events and send to buffer
        MessageType messageType = MessageType.from((char) msg.get());
        switch (messageType) {
            case BEGIN:
                handleMessageWithRetries(msg, this::processBeginMessage, messageType);
                break;
            case RELATION:
                handleMessageWithRetries(msg, this::processRelationMessage, messageType);
                break;
            case INSERT:
                handleMessageWithRetries(msg, this::processInsertMessage, messageType);
                break;
            case UPDATE:
                handleMessageWithRetries(msg, this::processUpdateMessage, messageType);
                break;
            case DELETE:
                handleMessageWithRetries(msg, this::processDeleteMessage, messageType);
                break;
            case COMMIT:
                handleMessageWithRetries(msg, this::processCommitMessage, messageType);
                break;
            case TYPE:
                handleMessageWithRetries(msg, this::processTypeMessage, messageType);
                break;
            default:
                throw new IllegalArgumentException("Replication message type [" + messageType + "] is not supported. ");
        }
    }

    public void stopClient() {
        try {
            logicalReplicationClient.disconnect();
            LOG.info("Logical replication client disconnected.");
        } catch (Exception e) {
            LOG.error("Logical replication client failed to disconnect.", e);
        }
    }

    public void stopCheckpointManager() {
        streamCheckpointManager.stop();
    }

    void processBeginMessage(ByteBuffer msg) {
        currentLsn = msg.getLong();
        long epochMicro = msg.getLong();
        currentEventTimestamp = convertPostgresEventTimestamp(epochMicro);
        int transaction_xid = msg.getInt();

        LOG.debug("Processed BEGIN message with LSN: {}, Timestamp: {}, TransactionId: {}", currentLsn, currentEventTimestamp, transaction_xid);
    }

    void processRelationMessage(ByteBuffer msg) {
        int tableId = msg.getInt();
        // null terminated string
        final String databaseName = getDatabaseName(sourceConfig.getTableNames());
        String schemaName = getNullTerminatedString(msg);
        String tableName = getNullTerminatedString(msg);
        int replicaId = msg.get();
        short numberOfColumns = msg.getShort();

        List<String> columnNames = new ArrayList<>();
        List<String> columnTypes = new ArrayList<>();
        for (int i = 0; i < numberOfColumns; i++) {
            int flag = msg.get();    // 1 indicates this column is part of the replica identity
            // null terminated string
            String columnName = getNullTerminatedString(msg);
            ColumnType columnType;
            try {
                columnType = ColumnType.getByTypeId(msg.getInt());
            } catch (IllegalArgumentException e) {
                final Set<String> enumColumns = getEnumColumns(databaseName, schemaName, tableName);
                if (enumColumns != null && enumColumns.contains(columnName)) {
                    columnType = ColumnType.getByTypeId(ColumnType.ENUM_TYPE_ID);
                } else throw e;
            }
            String columnTypeName = columnType.getTypeName();
            columnTypes.add(columnTypeName);
            int typeModifier = msg.getInt();
            if (columnType == ColumnType.VARCHAR) {
                int varcharLength = typeModifier - 4;
            } else if (columnType == ColumnType.NUMERIC) {
                int precision = (typeModifier - 4) >> 16;
                int scale = (typeModifier - 4) & 0xFFFF;
            }
            columnNames.add(columnName);
        }

        final List<String> primaryKeys = getPrimaryKeys(databaseName, schemaName, tableName);
        final TableMetadata tableMetadata = TableMetadata.builder()
                .withDatabaseName(databaseName)
                .withSchemaName(schemaName)
                .withTableName(tableName)
                .withColumnNames(columnNames)
                .withColumnTypes(columnTypes)
                .withPrimaryKeys(primaryKeys)
                .build();

        tableMetadataMap.put((long) tableId, tableMetadata);

        LOG.debug("Processed an Relation message with RelationId: {} Namespace: {} RelationName: {} ReplicaId: {}", tableId, schemaName, tableName, replicaId);
    }

    void processCommitMessage(ByteBuffer msg) {
        int flag = msg.get();
        long commitLsn = msg.getLong();
        long endLsn = msg.getLong();
        long epochMicro = msg.getLong();

        if (currentLsn != commitLsn) {
            // This shouldn't happen
            pipelineEvents.clear();
            throw new RuntimeException("Commit LSN does not match current LSN, skipping");
        }

        final long recordCount = pipelineEvents.size();
        AcknowledgementSet acknowledgementSet = null;
        if (sourceConfig.isAcknowledgmentsEnabled()) {
            acknowledgementSet = streamCheckpointManager.createAcknowledgmentSet(LogSequenceNumber.valueOf(currentLsn), recordCount);
        }

        writeToBuffer(bufferAccumulator, acknowledgementSet);
        bytesProcessedSummary.record(bytesReceived);
        LOG.debug("Processed a COMMIT message with Flag: {} CommitLsn: {} EndLsn: {} Timestamp: {}", flag, commitLsn, endLsn, epochMicro);

        if (sourceConfig.isAcknowledgmentsEnabled()) {
            acknowledgementSet.complete();
        } else {
            streamCheckpointManager.saveChangeEventsStatus(LogSequenceNumber.valueOf(currentLsn), recordCount);
        }
    }

    void processInsertMessage(ByteBuffer msg) {
        int tableId = msg.getInt();
        char n_char = (char) msg.get();  // Skip the 'N' character

        final TableMetadata tableMetadata = tableMetadataMap.get((long) tableId);
        final List<String> columnNames = tableMetadata.getColumnNames();
        final List<String> primaryKeys = tableMetadata.getPrimaryKeys();
        final long eventTimestampMillis = currentEventTimestamp;

        doProcess(msg, columnNames, tableMetadata, primaryKeys, eventTimestampMillis, OpenSearchBulkActions.INDEX);
        LOG.debug("Processed an INSERT message with table id: {}", tableId);
    }

    void processUpdateMessage(ByteBuffer msg) {
        final int tableId = msg.getInt();

        final TableMetadata tableMetadata = tableMetadataMap.get((long) tableId);
        final List<String> columnNames = tableMetadata.getColumnNames();
        final List<String> primaryKeys = tableMetadata.getPrimaryKeys();
        final List<String> columnTypes = tableMetadata.getColumnTypes();
        final long eventTimestampMillis = currentEventTimestamp;

        TupleDataType tupleDataType = TupleDataType.fromValue((char) msg.get());
        if (tupleDataType == TupleDataType.NEW) {
            doProcess(msg, columnNames, tableMetadata, primaryKeys, eventTimestampMillis, OpenSearchBulkActions.INDEX);
        } else if (tupleDataType == TupleDataType.OLD || tupleDataType == TupleDataType.KEY) {
            // Replica Identity is set to full, containing both old and new row data
            Map<String, Object> oldRowDataMap = getRowDataMap(msg, columnNames, columnTypes);
            msg.get();  // should be a char 'N'
            Map<String, Object> newRowDataMap = getRowDataMap(msg, columnNames, columnTypes);

            if (isPrimaryKeyChanged(oldRowDataMap, newRowDataMap, primaryKeys)) {
                LOG.debug("Primary keys were changed");
                createPipelineEvent(oldRowDataMap, tableMetadata, primaryKeys, eventTimestampMillis, OpenSearchBulkActions.DELETE);
            }
            createPipelineEvent(newRowDataMap, tableMetadata, primaryKeys, eventTimestampMillis, OpenSearchBulkActions.INDEX);
        }
        LOG.debug("Processed an UPDATE message with table id: {}", tableId);
    }

    private boolean isPrimaryKeyChanged(Map<String, Object> oldRowDataMap, Map<String, Object> newRowDataMap, List<String> primaryKeys) {
        for (String primaryKey : primaryKeys) {
            if (!oldRowDataMap.get(primaryKey).equals(newRowDataMap.get(primaryKey))) {
                return true;
            }
        }
        return false;
    }

    void processDeleteMessage(ByteBuffer msg) {
        int tableId = msg.getInt();
        char n_char = (char) msg.get();  // Skip the 'N' character

        final TableMetadata tableMetadata = tableMetadataMap.get((long) tableId);
        final List<String> columnNames = tableMetadata.getColumnNames();
        final List<String> primaryKeys = tableMetadata.getPrimaryKeys();
        final long eventTimestampMillis = currentEventTimestamp;

        doProcess(msg, columnNames, tableMetadata, primaryKeys, eventTimestampMillis, OpenSearchBulkActions.DELETE);
        LOG.debug("Processed a DELETE message with table id: {}", tableId);
    }

     void processTypeMessage(ByteBuffer msg) {
        int dataTypeId = msg.getInt();
        String schemaName = getNullTerminatedString(msg);
        String typeName = getNullTerminatedString(msg);
        LOG.debug("Processed a TYPE message with TypeId: {} Namespace: {} TypeName: {}", dataTypeId, schemaName, typeName);
    }

    private void doProcess(ByteBuffer msg, List<String> columnNames, TableMetadata tableMetadata,
                           List<String> primaryKeys, long eventTimestampMillis, OpenSearchBulkActions bulkAction) {
        bytesReceived = msg.capacity();
        bytesReceivedSummary.record(bytesReceived);
        final List<String> columnTypes = tableMetadata.getColumnTypes();
        Map<String, Object> rowDataMap = getRowDataMap(msg, columnNames, columnTypes);
        createPipelineEvent(rowDataMap, tableMetadata, primaryKeys, eventTimestampMillis, bulkAction);
    }

    private Map<String, Object> getRowDataMap(ByteBuffer msg, List<String> columnNames, List<String> columnTypes) {
        Map<String, Object> rowDataMap = new HashMap<>();
        short numberOfColumns = msg.getShort();
        for (int i = 0; i < numberOfColumns; i++) {
            char type = (char) msg.get();
            if (type == 'n') {
                rowDataMap.put(columnNames.get(i), null);
            } else if (type == 't') {
                int length = msg.getInt();
                byte[] bytes = new byte[length];
                msg.get(bytes);
                final String value = new String(bytes, StandardCharsets.UTF_8);
                final String columnName = columnNames.get(i);
                final String columnType = columnTypes.get(i);
                final Object data = PostgresDataTypeHelper.getDataByColumnType(PostgresDataType.byDataType(columnType), columnName,
                        value);
                rowDataMap.put(columnNames.get(i), data);
            } else {
                LOG.warn("Unknown column type: {}", type);
            }
        }
        return rowDataMap;
    }

    private void createPipelineEvent(Map<String, Object> rowDataMap, TableMetadata tableMetadata, List<String> primaryKeys, long eventTimestampMillis, OpenSearchBulkActions bulkAction) {
        final Event dataPrepperEvent = JacksonEvent.builder()
                .withEventType("event")
                .withData(rowDataMap)
                .build();

        final Event pipelineEvent = recordConverter.convert(
                dataPrepperEvent,
                tableMetadata.getDatabaseName(),
                tableMetadata.getSchemaName(),
                tableMetadata.getTableName(),
                bulkAction,
                primaryKeys,
                eventTimestampMillis,
                eventTimestampMillis,
                null);
        pipelineEvents.add(pipelineEvent);
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

    private long convertPostgresEventTimestamp(long postgresMicro) {
        // Offset in microseconds between 1970-01-01 and 2000-01-01
        long offsetMicro = 946684800L * 1_000_000L;
        return (postgresMicro + offsetMicro) / 1000;
    }

    private String getNullTerminatedString(ByteBuffer msg) {
        StringBuilder sb = new StringBuilder();
        while (msg.hasRemaining()) {
            byte b = msg.get();
            if (b == 0) break; // Stop at null terminator
            sb.append((char) b);
        }
        return sb.toString();
    }

    private List<String> getPrimaryKeys(String databaseName, String schemaName, String tableName) {
        StreamProgressState progressState = streamPartition.getProgressState().get();
        return progressState.getPrimaryKeyMap().get(databaseName + "." + schemaName + "." + tableName);
    }

    private Set<String> getEnumColumns(String databaseName, String schemaName, String tableName) {
        StreamProgressState progressState = streamPartition.getProgressState().get();
        return progressState.getPostgresStreamState().getEnumColumnsByTable().get(databaseName + "." + schemaName + "." + tableName);
    }

    private String getDatabaseName(List<String> tableNames) {
        return tableNames.get(0).split(DOT_DELIMITER_REGEX)[0];
    }

    private void handleMessageWithRetries(ByteBuffer message, Consumer<ByteBuffer> function, MessageType messageType) {
        int retry = 0;
        while (retry <= NUM_OF_RETRIES) {
            try {
                eventProcessingTimer.record(() -> function.accept(message));
                return;
            } catch (Exception e) {
                LOG.warn("Error when processing change event of type {}, will retry", messageType, e);
                applyBackoff();
            }
            retry++;
        }
        LOG.error("Failed to process change event of type {} after {} retries", messageType, NUM_OF_RETRIES);
        eventProcessingErrorCounter.increment();
    }

    private void applyBackoff() {
        try {
            Thread.sleep(BACKOFF_IN_MILLIS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
