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
import org.opensearch.dataprepper.plugins.source.rds.model.MessageType;
import org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata;
import org.postgresql.replication.LogSequenceNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private long currentLsn;
    private long currentEventTimestamp;

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
                acknowledgementSetManager, this::stopClient, sourceConfig.getStreamAcknowledgmentTimeout(), sourceConfig.getEngine());
        streamCheckpointManager.start();

        tableMetadataMap = new HashMap<>();
        pipelineEvents = new ArrayList<>();
    }

    public void process(ByteBuffer msg) {
        // Message processing logic:
        // If it's a BEGIN, note its LSN
        // If it's a RELATION, update table metadata map
        // If it's INSERT/UPDATE/DELETE, prepare events
        // If it's a COMMIT, convert all prepared events and send to buffer
        MessageType messageType = MessageType.from((char) msg.get());
        if (messageType == MessageType.BEGIN) {
            processBeginMessage(msg);
        } else if (messageType == MessageType.RELATION) {
            processRelationMessage(msg);
        } else if (messageType == MessageType.INSERT) {
            processInsertMessage(msg);
        } else if (messageType == MessageType.UPDATE) {
            processUpdateMessage(msg);
        } else if (messageType == MessageType.DELETE) {
            processDeleteMessage(msg);
        } else if (messageType == MessageType.COMMIT) {
            processCommitMessage(msg);
        } else {
            throw new IllegalArgumentException("Replication message type [" + messageType + "] is not supported. ");
        }
    }

    public void stopClient() {
        try {
            logicalReplicationClient.disconnect();
            LOG.info("Binary log client disconnected.");
        } catch (Exception e) {
            LOG.error("Binary log client failed to disconnect.", e);
        }
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
        String schemaName = getNullTerminatedString(msg);
        String tableName = getNullTerminatedString(msg);
        int replicaId = msg.get();
        short numberOfColumns = msg.getShort();

        List<String> columnNames = new ArrayList<>();
        for (int i = 0; i < numberOfColumns; i++) {
            int flag = msg.get();    // 1 indicates this column is part of the replica identity
            // null terminated string
            String columnName = getNullTerminatedString(msg);
            ColumnType columnType = ColumnType.getByTypeId(msg.getInt());
            String columnTypeName = columnType.getTypeName();
            int typeModifier = msg.getInt();
            if (columnType == ColumnType.VARCHAR) {
                int varcharLength = typeModifier - 4;
            } else if (columnType == ColumnType.NUMERIC) {
                int precision = (typeModifier - 4) >> 16;
                int scale = (typeModifier - 4) & 0xFFFF;
            }
            columnNames.add(columnName);
        }

        final List<String> primaryKeys = getPrimaryKeys(schemaName, tableName);
        final TableMetadata tableMetadata = new TableMetadata(
                tableName, schemaName, columnNames, primaryKeys);

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

        AcknowledgementSet acknowledgementSet = null;
        if (sourceConfig.isAcknowledgmentsEnabled()) {
            acknowledgementSet = streamCheckpointManager.createAcknowledgmentSet(LogSequenceNumber.valueOf(currentLsn));
        }

        writeToBuffer(bufferAccumulator, acknowledgementSet);
        LOG.debug("Processed a COMMIT message with Flag: {} CommitLsn: {} EndLsn: {} Timestamp: {}", flag, commitLsn, endLsn, epochMicro);

        if (sourceConfig.isAcknowledgmentsEnabled()) {
            acknowledgementSet.complete();
        } else {
            streamCheckpointManager.saveChangeEventsStatus(LogSequenceNumber.valueOf(currentLsn));
        }
    }

    void processInsertMessage(ByteBuffer msg) {
        int tableId = msg.getInt();
        char n_char = (char) msg.get();  // Skip the 'N' character

        final TableMetadata tableMetadata = tableMetadataMap.get((long)tableId);
        final List<String> columnNames = tableMetadata.getColumnNames();
        final List<String> primaryKeys = tableMetadata.getPrimaryKeys();
        final long eventTimestampMillis = currentEventTimestamp;

        doProcess(msg, columnNames, tableMetadata, primaryKeys, eventTimestampMillis, OpenSearchBulkActions.INDEX);
        LOG.debug("Processed an INSERT message with table id: {}", tableId);
    }

    void processUpdateMessage(ByteBuffer msg) {
        final int tableId = msg.getInt();

        final TableMetadata tableMetadata = tableMetadataMap.get((long)tableId);
        final List<String> columnNames = tableMetadata.getColumnNames();
        final List<String> primaryKeys = tableMetadata.getPrimaryKeys();
        final long eventTimestampMillis = currentEventTimestamp;

        TupleDataType tupleDataType = TupleDataType.fromValue((char) msg.get());
        if (tupleDataType == TupleDataType.NEW) {
            doProcess(msg, columnNames, tableMetadata, primaryKeys, eventTimestampMillis, OpenSearchBulkActions.INDEX);
            LOG.debug("Processed an UPDATE message with table id: {}", tableId);
        } else if (tupleDataType == TupleDataType.KEY) {
            // Primary keys were changed
            doProcess(msg, columnNames, tableMetadata, primaryKeys, eventTimestampMillis, OpenSearchBulkActions.DELETE);
            msg.get();  // should be a char 'N'
            doProcess(msg, columnNames, tableMetadata, primaryKeys, eventTimestampMillis, OpenSearchBulkActions.INDEX);
            LOG.debug("Processed an UPDATE message with table id: {} and primary key(s) were changed", tableId);

        } else if (tupleDataType == TupleDataType.OLD) {
            // Replica Identity is set to full, containing both old and new row data
            Map<String, Object> oldRowDataMap = getRowDataMap(msg, columnNames);
            msg.get();  // should be a char 'N'
            Map<String, Object> newRowDataMap = getRowDataMap(msg, columnNames);

            if (isPrimaryKeyChanged(oldRowDataMap, newRowDataMap, primaryKeys)) {
                createPipelineEvent(oldRowDataMap, tableMetadata, primaryKeys, eventTimestampMillis, OpenSearchBulkActions.DELETE);
            }
            createPipelineEvent(newRowDataMap, tableMetadata, primaryKeys, eventTimestampMillis, OpenSearchBulkActions.INDEX);
        }
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

        final TableMetadata tableMetadata = tableMetadataMap.get((long)tableId);
        final List<String> columnNames = tableMetadata.getColumnNames();
        final List<String> primaryKeys = tableMetadata.getPrimaryKeys();
        final long eventTimestampMillis = currentEventTimestamp;

        doProcess(msg, columnNames, tableMetadata, primaryKeys, eventTimestampMillis, OpenSearchBulkActions.DELETE);
        LOG.debug("Processed a DELETE message with table id: {}", tableId);
    }

    private void doProcess(ByteBuffer msg, List<String> columnNames, TableMetadata tableMetadata,
                           List<String> primaryKeys, long eventTimestampMillis, OpenSearchBulkActions bulkAction) {
        Map<String, Object> rowDataMap = getRowDataMap(msg, columnNames);

        createPipelineEvent(rowDataMap, tableMetadata, primaryKeys, eventTimestampMillis, bulkAction);
    }

    private Map<String, Object> getRowDataMap(ByteBuffer msg, List<String> columnNames) {
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
                rowDataMap.put(columnNames.get(i), new String(bytes));
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
        } catch (Exception e) {
            // this will only happen if writing to buffer gets interrupted from shutdown,
            // otherwise bufferAccumulator will keep retrying with backoff
            LOG.error("Failed to flush buffer", e);
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

    private List<String> getPrimaryKeys(String schemaName, String tableName) {
        final String databaseName = sourceConfig.getTableNames().get(0).split("\\.")[0];
        StreamProgressState progressState = streamPartition.getProgressState().get();

        return progressState.getPrimaryKeyMap().get(databaseName + "." + schemaName + "." + tableName);
    }
}
