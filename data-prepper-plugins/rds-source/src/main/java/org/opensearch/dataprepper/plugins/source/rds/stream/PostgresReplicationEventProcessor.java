package org.opensearch.dataprepper.plugins.source.rds.stream;

import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.opensearch.dataprepper.plugins.source.rds.converter.StreamRecordConverter;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.rds.datatype.postgres.ColumnType;
import org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PostgresReplicationEventProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresReplicationEventProcessor.class);

    static final Duration BUFFER_TIMEOUT = Duration.ofSeconds(60);
    static final int DEFAULT_BUFFER_BATCH_SIZE = 1_000;

    private final StreamPartition streamPartition;
    private final StreamRecordConverter recordConverter;
    private final Buffer<Record<Event>> buffer;
    private final BufferAccumulator<Record<Event>> bufferAccumulator;
    private final List<Event> pipelineEvents;

    private long currentLsn;
    private long currentEventTimestamp;

    private Map<Long, TableMetadata> tableMetadataMap;

    public PostgresReplicationEventProcessor(final StreamPartition streamPartition,
                                             final RdsSourceConfig sourceConfig,
                                             final Buffer<Record<Event>> buffer,
                                             final String s3Prefix) {
        this.streamPartition = streamPartition;
        recordConverter = new StreamRecordConverter(s3Prefix, sourceConfig.getPartitionCount());
        this.buffer = buffer;
        bufferAccumulator = BufferAccumulator.create(buffer, DEFAULT_BUFFER_BATCH_SIZE, BUFFER_TIMEOUT);

        tableMetadataMap = new HashMap<>();
        pipelineEvents = new ArrayList<>();
    }

    public void process(ByteBuffer msg) {
        // Message processing logic:
        // If it's a BEGIN, note its LSN
        // If it's a RELATION, update table metadata map
        // If it's INSERT/UPDATE/DELETE, prepare events
        // If it's a COMMIT, convert all prepared events and send to buffer
        char messageType = (char) msg.get();
        if (messageType == 'B') {
            processBeginMessage(msg);
        } else if (messageType == 'R') {
            processRelationMessage(msg);
        } else if (messageType == 'I') {
            processInsertMessage(msg);
        } else if (messageType == 'U') {
            processUpdateMessage(msg);
        } else if (messageType == 'D') {
            processDeleteMessage(msg);
        } else if (messageType == 'C') {
            processCommitMessage(msg);
        }
    }

    private void processCommitMessage(ByteBuffer msg) {
        int flag = msg.get();
        long commitLsn = msg.getLong();
        long endLsn = msg.getLong();
        long epochMicro = msg.getLong();

        if (currentLsn != commitLsn) {
            // This shouldn't happen
            LOG.warn("Commit LSN does not match current LSN, skipping");
            pipelineEvents.clear();
            return;
        }

        writeToBuffer(bufferAccumulator);
        LOG.info("Processed a COMMIT message with Flag: {} CommitLsn: {} EndLsn: {} Timestamp: {}", flag, commitLsn, endLsn, epochMicro);
    }

    private void writeToBuffer(BufferAccumulator<Record<Event>> bufferAccumulator) {
        for (Event pipelineEvent : pipelineEvents) {
            addToBufferAccumulator(bufferAccumulator, new Record<>(pipelineEvent));
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

    private void processDeleteMessage(ByteBuffer msg) {
        int tableId = msg.getInt();
        char typeId = (char) msg.get();

        final TableMetadata tableMetadata = tableMetadataMap.get((long)tableId);
        final List<String> columnNames = tableMetadata.getColumnNames();
        final List<String> primaryKeys = tableMetadata.getPrimaryKeys();
        final long eventTimestampMillis = currentEventTimestamp;

        short numberOfColumns = msg.getShort();
        System.out.println("Column length is: " + numberOfColumns);
        Map<String, Object> rowDataMap = new HashMap<>();
        for (int i = 0; i < numberOfColumns; i++) {
            char type = (char) msg.get();
            String columnName = columnNames.get(i);
            if (type == 'n') {
                rowDataMap.put(columnName, null);
            } else if (type == 't') {
                int length = msg.getInt();
                byte[] bytes = new byte[length];
                msg.get(bytes);
                rowDataMap.put(columnName, new String(bytes));
            } else if (type == 'u') {
                // TODO: put original value here; this should only happen if not all column data is present in
                // the UPDATE message
                rowDataMap.put(columnName, "UNCHANGED");
            } else {
                LOG.warn("Unknown column type: {}", type);
            }
        }

        final Event dataPrepperEvent = JacksonEvent.builder()
                .withEventType("event")
                .withData(rowDataMap)
                .build();

        final Event pipelineEvent = recordConverter.convert(
                dataPrepperEvent,
                tableMetadata.getDatabaseName(),
                tableMetadata.getTableName(),
                OpenSearchBulkActions.INDEX,
                primaryKeys,
                eventTimestampMillis,
                eventTimestampMillis,
                null);
        pipelineEvents.add(pipelineEvent);

        LOG.info("Processed a DELETE message with RelationId: " + tableId + " TypeId: " + typeId +
                " DeletedColumnValues: " + rowDataMap);
    }

    private void processUpdateMessage(ByteBuffer msg) {
        int tableId = msg.getInt();
        char typeId = (char) msg.get();

        final TableMetadata tableMetadata = tableMetadataMap.get((long)tableId);
        final List<String> columnNames = tableMetadata.getColumnNames();
        final List<String> primaryKeys = tableMetadata.getPrimaryKeys();
        final long eventTimestampMillis = currentEventTimestamp;

        if (typeId == 'K') {
            // TODO
        } else if (typeId == 'O') {
            // TODO
        } else if (typeId == 'N') {
            Map<String, Object> rowDataMap = new HashMap<>();
            short numberOfColumns = msg.getShort();
            for (int i = 0; i < numberOfColumns; i++) {
                char type = (char) msg.get();
                String columnName = columnNames.get(i);
                if (type == 'n') {
                    rowDataMap.put(columnName, null);
                } else if (type == 't') {
                    int length = msg.getInt();
                    byte[] bytes = new byte[length];
                    msg.get(bytes);
                    rowDataMap.put(columnName, new String(bytes));
                } else if (type == 'u') {
                    // TODO: put original value here; this should only happen if not all column data is present in
                    // the UPDATE message
                    rowDataMap.put(columnName, "UNCHANGED");
                } else {
                    LOG.warn("Unknown column type: {}", type);
                }
            }

            final Event dataPrepperEvent = JacksonEvent.builder()
                    .withEventType("event")
                    .withData(rowDataMap)
                    .build();

            final Event pipelineEvent = recordConverter.convert(
                    dataPrepperEvent,
                    tableMetadata.getDatabaseName(),
                    tableMetadata.getTableName(),
                    OpenSearchBulkActions.INDEX,
                    primaryKeys,
                    eventTimestampMillis,
                    eventTimestampMillis,
                    null);
            pipelineEvents.add(pipelineEvent);

            LOG.info("Processed an UPDATE message with RelationId: {} UpdatedColumnValues: {}", tableId, rowDataMap);
        }
    }

    private void processInsertMessage(ByteBuffer msg) {
        int tableId = msg.getInt();
        char n_char = (char) msg.get();  // Skip the 'N' character
        short numberOfColumns = msg.getShort();

        final TableMetadata tableMetadata = tableMetadataMap.get((long)tableId);
        final List<String> columnNames = tableMetadata.getColumnNames();
        final List<String> primaryKeys = tableMetadata.getPrimaryKeys();
        final long eventTimestampMillis = currentEventTimestamp;

        Map<String, Object> rowDataMap = new HashMap<>();
        for (int i = 0; i < numberOfColumns; i++) {
            char type = (char) msg.get();
            if (type == 'n') {
                rowDataMap.put(columnNames.get(i), null);
            } else if (type == 't') {
                int length = msg.getInt();
                byte[] bytes = new byte[length];
                msg.get(bytes);
                rowDataMap.put(columnNames.get(i), new String(bytes));
            } else if (type == 'u') {
                rowDataMap.put(columnNames.get(i), "UNCHANGED");
            } else {
                LOG.warn("Unknown column type: {}", type);
            }
        }

        final Event dataPrepperEvent = JacksonEvent.builder()
                .withEventType("event")
                .withData(rowDataMap)
                .build();

        final Event pipelineEvent = recordConverter.convert(
                dataPrepperEvent,
                tableMetadata.getDatabaseName(),
                tableMetadata.getTableName(),
                OpenSearchBulkActions.INDEX,
                primaryKeys,
                eventTimestampMillis,
                eventTimestampMillis,
                null);
        pipelineEvents.add(pipelineEvent);

        LOG.info("Processed an INSERT message with RelationOid: {} ColumnValues: {}", tableId, rowDataMap);
    }

    private void processRelationMessage(ByteBuffer msg) {
        int tableId = msg.getInt();
        // null terminated string
        String schemaName = getNullTerminatedString(msg);
        String tableName = getNullTerminatedString(msg);
        int replicaId = msg.get();
        short numberOfColumns = msg.getShort();

        List<String> columnNames = new ArrayList<>();
        List<String> columnsInfo = new ArrayList<>();
        for (int i = 0; i < numberOfColumns; i++) {
            String columnInfo;
            int flag = msg.get();    // 1 indicates this column is part of the replica identity
            // null terminated string
            String columnName = getNullTerminatedString(msg);
            ColumnType columnType = ColumnType.getByTypeId(msg.getInt());
            String columnTypeName = columnType.getTypeName();
            int typeModifier = msg.getInt();
            if (columnType == ColumnType.VARCHAR) {
                int varcharLength = typeModifier - 4;
                columnInfo = columnName + " (varchar with length " + varcharLength + ")";
            } else if (columnType == ColumnType.NUMERIC) {
                int precision = (typeModifier - 4) >> 16;
                int scale = (typeModifier - 4) & 0xFFFF;
                columnInfo = columnName + " (numeric with precision " + precision + ", scale " + scale + ")";
            } else {
                columnInfo = columnName + " (" + columnTypeName + ")";
            }
            columnsInfo.add(columnInfo);
            columnNames.add(columnName);
        }

        // TODO: get primary key beforehand
        final TableMetadata tableMetadata = new TableMetadata(
                tableName, schemaName, columnNames, List.of("id"));

        tableMetadataMap.put((long) tableId, tableMetadata);

        LOG.info("Processed an Relation message with RelationId: {} Namespace: {} RelationName: {} ReplicaId: {} ColumnsInfo: {}", tableId, schemaName, tableName, replicaId, columnsInfo);
    }

    private void processBeginMessage(ByteBuffer msg) {
        currentLsn = msg.getLong();
        long epochMicro = msg.getLong();
        currentEventTimestamp = convertPostgresEventTimestamp(epochMicro);
        int transaction_xid = msg.getInt();

        LOG.info("Processed BEGIN message with LSN: {}, Timestamp: {}, TransactionId: {}", currentLsn, currentEventTimestamp, transaction_xid);
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
}
