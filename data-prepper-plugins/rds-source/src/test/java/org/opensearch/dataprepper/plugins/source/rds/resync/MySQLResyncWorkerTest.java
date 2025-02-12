/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.resync;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.opensearch.OpenSearchBulkActions;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.opensearch.dataprepper.plugins.source.rds.converter.RecordConverter;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.ResyncPartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.ResyncProgressState;
import org.opensearch.dataprepper.plugins.source.rds.model.DbTableMetadata;
import org.opensearch.dataprepper.plugins.source.rds.schema.QueryManager;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.rds.model.TableMetadata.DOT_DELIMITER;

@ExtendWith(MockitoExtension.class)
class MySQLResyncWorkerTest {

    @Mock
    private ResyncPartition resyncPartition;

    @Mock
    private RdsSourceConfig sourceConfig;

    @Mock
    private QueryManager queryManager;

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private RecordConverter recordConverter;

    @Mock
    private AcknowledgementSet acknowledgementSet;

    @Mock
    private DbTableMetadata dbTableMetadata;

    private MySQLResyncWorker resyncWorker;

    @BeforeEach
    void setUp() {
        resyncWorker = createObjectUnderTest();
    }

    @Test
    void test_run_process_resync_with_acknowledgments_enabled() throws Exception {
        final String database = "test-database";
        final String table = "test-table";
        final long eventTimestampMillis = 1234567890L;
        final ResyncPartition.PartitionKeyInfo partitionKeyInfo = mock(ResyncPartition.PartitionKeyInfo.class);
        final ResyncProgressState progressState = mock(ResyncProgressState.class);
        final String foreignKeyName = "test-foreign-key";
        final Object updatedValue = "test-updated-value";
        final String queryStatement = "SELECT * FROM " + database + "." + table + " WHERE " + foreignKeyName + "='" + updatedValue + "'";
        final String primaryKeyName = "test-primary-key";
        final String primaryKeyValue = "test-primary-key-value";
        final Map<String, Object> rowData = Map.of(
                primaryKeyName, primaryKeyValue,
                foreignKeyName, updatedValue
        );
        final List<Map<String, Object>> rows = List.of(rowData);
        final Map<String, Map<String, String>> tableColumnTypeMap = Map.of(
                database + DOT_DELIMITER + table, Map.of(
                        primaryKeyName, "varchar",
                        foreignKeyName, "varchar"
                )
        );
        final Event dataPrepperEvent = mock(Event.class);

        when(resyncPartition.getPartitionKeyInfo()).thenReturn(partitionKeyInfo);
        when(partitionKeyInfo.getDatabase()).thenReturn(database);
        when(partitionKeyInfo.getTable()).thenReturn(table);
        when(partitionKeyInfo.getTimestamp()).thenReturn(eventTimestampMillis);
        when(resyncPartition.getProgressState()).thenReturn(Optional.of(progressState));
        when(progressState.getForeignKeyName()).thenReturn(foreignKeyName);
        when(progressState.getUpdatedValue()).thenReturn(updatedValue);
        when(progressState.getPrimaryKeys()).thenReturn(List.of(primaryKeyName));
        when(queryManager.selectRows(queryStatement)).thenReturn(rows);
        when(dbTableMetadata.getTableColumnDataTypeMap()).thenReturn(tableColumnTypeMap);
        when(recordConverter.convert(any(Event.class), eq(database), eq(table), eq(OpenSearchBulkActions.INDEX),
                eq(List.of(primaryKeyName)), eq(eventTimestampMillis), eq(eventTimestampMillis), eq(null)))
                .thenReturn(dataPrepperEvent);

        final BufferAccumulator<Record<Event>> bufferAccumulator = mock(BufferAccumulator.class);
        try (final MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class)) {
            bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(buffer, MySQLResyncWorker.DEFAULT_BUFFER_BATCH_SIZE, MySQLResyncWorker.BUFFER_TIMEOUT))
                    .thenReturn(bufferAccumulator);
            resyncWorker.run();
        }

        verify(acknowledgementSet).add(dataPrepperEvent);

        ArgumentCaptor<Record<Event>> recordArgumentCaptor = ArgumentCaptor.forClass(Record.class);
        verify(bufferAccumulator).add(recordArgumentCaptor.capture());
        final Record<Event> record = recordArgumentCaptor.getValue();
        assertThat(record.getData(), is(dataPrepperEvent));

        verify(bufferAccumulator).flush();
        verify(acknowledgementSet).complete();
    }

    private MySQLResyncWorker createObjectUnderTest() {
        return MySQLResyncWorker.create(resyncPartition, sourceConfig, queryManager, buffer, recordConverter, acknowledgementSet, dbTableMetadata);
    }
}
