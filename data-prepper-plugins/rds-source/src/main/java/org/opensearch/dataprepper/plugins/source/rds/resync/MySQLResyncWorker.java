/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.resync;

import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.opensearch.dataprepper.plugins.source.rds.converter.RecordConverter;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.ResyncPartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.ResyncProgressState;
import org.opensearch.dataprepper.plugins.source.rds.datatype.mysql.MySQLDataType;
import org.opensearch.dataprepper.plugins.source.rds.datatype.mysql.MySQLDataTypeHelper;
import org.opensearch.dataprepper.plugins.source.rds.model.DbTableMetadata;
import org.opensearch.dataprepper.plugins.source.rds.schema.QueryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata.DOT_DELIMITER;

public class MySQLResyncWorker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(MySQLResyncWorker.class);
    private static final String QUERY_NULL_FORMAT_STRING = "SELECT * FROM %s WHERE %s IS NULL";
    private static final String QUERY_NOT_NULL_FORMAT_STRING = "SELECT * FROM %s WHERE %s='%s'";

    static final String DATA_PREPPER_EVENT_TYPE = "event";
    static final Duration BUFFER_TIMEOUT = Duration.ofSeconds(60);
    static final int DEFAULT_BUFFER_BATCH_SIZE = 1_000;

    private final ResyncPartition resyncPartition;
    private final RdsSourceConfig sourceConfig;
    private final QueryManager queryManager;
    private final Buffer<Record<Event>> buffer;
    private final RecordConverter recordConverter;
    private final AcknowledgementSet acknowledgementSet;
    private final DbTableMetadata dbTableMetadata;

    MySQLResyncWorker(ResyncPartition resyncPartition,
                 RdsSourceConfig sourceConfig,
                 QueryManager queryManager,
                 Buffer<Record<Event>> buffer,
                 RecordConverter recordConverter,
                 AcknowledgementSet acknowledgementSet,
                 DbTableMetadata dbTableMetadata) {
        this.resyncPartition = resyncPartition;
        this.sourceConfig = sourceConfig;
        this.queryManager = queryManager;
        this.buffer = buffer;
        this.recordConverter = recordConverter;
        this.acknowledgementSet = acknowledgementSet;
        this.dbTableMetadata = dbTableMetadata;
    }

    public static MySQLResyncWorker create(ResyncPartition resyncPartition,
                                      RdsSourceConfig sourceConfig,
                                      QueryManager queryManager,
                                      Buffer<Record<Event>> buffer,
                                      RecordConverter recordConverter,
                                      AcknowledgementSet acknowledgementSet,
                                      DbTableMetadata dbTableMetadata) {
        return new MySQLResyncWorker(resyncPartition, sourceConfig, queryManager, buffer, recordConverter, acknowledgementSet, dbTableMetadata);
    }

    public void run() {
        ResyncPartition.PartitionKeyInfo partitionKeyInfo = resyncPartition.getPartitionKeyInfo();
        final String database = partitionKeyInfo.getDatabase();
        final String table = partitionKeyInfo.getTable();
        final long eventTimestampMillis = partitionKeyInfo.getTimestamp();

        final ResyncProgressState progressState = validateAndGetProgressState();
        final String foreignKeyName = progressState.getForeignKeyName();
        final Object updatedValue = progressState.getUpdatedValue();
        final List<String> primaryKeys = progressState.getPrimaryKeys();

        final List<Map<String, Object>> rows = executeQuery(database, table, foreignKeyName, updatedValue);

        processRows(rows, database, table, primaryKeys, eventTimestampMillis);
    }

    private void processRows(List<Map<String, Object>> rows, String database, String table, List<String> primaryKeys, long eventTimestampMillis) {
        BufferAccumulator<Record<Event>> bufferAccumulator = BufferAccumulator.create(buffer, DEFAULT_BUFFER_BATCH_SIZE, BUFFER_TIMEOUT);

        for (Map<String, Object> row : rows) {
            final Event dataPrepperEvent = JacksonEvent.builder()
                    .withEventType(DATA_PREPPER_EVENT_TYPE)
                    .withData(mapDataType(row, database + DOT_DELIMITER + table))
                    .build();

            final Event pipelineEvent = recordConverter.convert(
                    dataPrepperEvent,
                    database,
                    database,
                    table,
                    OpenSearchBulkActions.INDEX,
                    primaryKeys,
                    eventTimestampMillis,
                    eventTimestampMillis,
                    null);

            if (acknowledgementSet != null) {
                acknowledgementSet.add(pipelineEvent);
            }

            try {
                bufferAccumulator.add(new Record<>(pipelineEvent));
            } catch (Exception e) {
                LOG.error("Failed to add event to buffer", e);
                throw new RuntimeException(e);
            }
        }

        try {
            bufferAccumulator.flush();
            if (acknowledgementSet != null) {
                acknowledgementSet.complete();
            }
        } catch (Exception e) {
            // this will only happen if writing to buffer gets interrupted from shutdown,
            // otherwise bufferAccumulator will keep retrying with backoff
            LOG.error("Failed to flush buffer", e);
        }
    }

    private ResyncProgressState validateAndGetProgressState() {
        return resyncPartition.getProgressState()
                .orElseThrow(() -> new IllegalStateException(
                        "ResyncPartition " + resyncPartition.getPartitionKey() + " doesn't contain progress state."));
    }

    private List<Map<String, Object>> executeQuery(String database, String table, String foreignKeyName, Object updatedValue) {
        LOG.debug("Will perform resync on table: {}.{}, with foreign key name: {}, and updated value: {}", database, table, foreignKeyName, updatedValue);
        final String fullTableName = database + DOT_DELIMITER + table;
        String queryStatement;
        if (updatedValue == null) {
            queryStatement = String.format(QUERY_NULL_FORMAT_STRING, fullTableName, foreignKeyName);
        } else {
            queryStatement = String.format(QUERY_NOT_NULL_FORMAT_STRING, fullTableName, foreignKeyName, updatedValue);
        }
        LOG.debug("Query statement: {}", queryStatement);

        List<Map<String, Object>> rows = queryManager.selectRows(queryStatement);
        LOG.debug("Found {} rows to resync", rows.size());
        return rows;
    }

    private Map<String, Object> mapDataType(final Map<String, Object> rowData, final String fullTableName) {
        Map<String, String> columnDataTypeMap = dbTableMetadata.getTableColumnDataTypeMap().get(fullTableName);
        Map<String, Object> rowDataAfterMapping = new HashMap<>();
        for (Map.Entry<String, Object> entry : rowData.entrySet()) {
            final Object data = MySQLDataTypeHelper.getDataByColumnType(MySQLDataType.byDataType(columnDataTypeMap.get(entry.getKey())), entry.getKey(),
                    entry.getValue(), null);
            rowDataAfterMapping.put(entry.getKey(), data);
        }
        return rowDataAfterMapping;
    }
}
