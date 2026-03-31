/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.iceberg.worker;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.iceberg.IcebergSourceConfig;
import org.opensearch.dataprepper.plugins.source.iceberg.TableConfig;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.partition.ChangelogTaskPartition;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.partition.InitialLoadTaskPartition;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.state.ChangelogTaskProgressState;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.state.InitialLoadTaskProgressState;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.iceberg.worker.ChangelogWorker.BYTES_PROCESSED;
import static org.opensearch.dataprepper.plugins.source.iceberg.worker.ChangelogWorker.CARRYOVER_ROWS_REMOVED;
import static org.opensearch.dataprepper.plugins.source.iceberg.worker.ChangelogWorker.CHANGE_EVENTS_PROCESSED_COUNT;
import static org.opensearch.dataprepper.plugins.source.iceberg.worker.ChangelogWorker.CHANGE_EVENTS_PROCESSING_ERROR_COUNT;
import static org.opensearch.dataprepper.plugins.source.iceberg.worker.ChangelogWorker.EXPORT_RECORDS_PROCESSED_COUNT;
import static org.opensearch.dataprepper.plugins.source.iceberg.worker.ChangelogWorker.EXPORT_RECORDS_PROCESSING_ERROR_COUNT;

@ExtendWith(MockitoExtension.class)
class ChangelogWorkerTest {

    private static final Schema TEST_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get())
    );

    @Mock
    private EnhancedSourceCoordinator sourceCoordinator;
    @Mock
    private IcebergSourceConfig sourceConfig;
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private Table table;
    @Mock
    private TableConfig tableConfig;
    @Mock
    private Buffer<org.opensearch.dataprepper.model.record.Record<Event>> buffer;
    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private IcebergDataFileReader dataFileReader;
    @Mock
    private Counter changeEventsProcessedCounter;
    @Mock
    private Counter changeEventsProcessingErrorCounter;
    @Mock
    private Counter exportRecordsProcessedCounter;
    @Mock
    private Counter exportRecordsProcessingErrorCounter;
    @Mock
    private DistributionSummary bytesProcessedSummary;
    @Mock
    private DistributionSummary carryoverRowsRemovedSummary;

    private static final String TABLE_NAME = "test_db.users";

    @BeforeEach
    void setUp() {
        when(pluginMetrics.counter(CHANGE_EVENTS_PROCESSED_COUNT)).thenReturn(changeEventsProcessedCounter);
        when(pluginMetrics.counter(CHANGE_EVENTS_PROCESSING_ERROR_COUNT)).thenReturn(changeEventsProcessingErrorCounter);
        when(pluginMetrics.counter(EXPORT_RECORDS_PROCESSED_COUNT)).thenReturn(exportRecordsProcessedCounter);
        when(pluginMetrics.counter(EXPORT_RECORDS_PROCESSING_ERROR_COUNT)).thenReturn(exportRecordsProcessingErrorCounter);
        when(pluginMetrics.summary(BYTES_PROCESSED)).thenReturn(bytesProcessedSummary);
        when(pluginMetrics.summary(CARRYOVER_ROWS_REMOVED)).thenReturn(carryoverRowsRemovedSummary);
    }

    @Test
    void processPartition_insertOnly_recordsChangeEventsAndBytes() throws Exception {
        final Record row1 = createRecord(1, "Alice");
        final Record row2 = createRecord(2, "Bob");

        final ChangelogTaskProgressState state = new ChangelogTaskProgressState();
        state.setTableName(TABLE_NAME);
        state.setSnapshotId(100L);
        state.setDataFilePaths(List.of("s3://bucket/file1.parquet"));
        state.setTaskTypes(List.of("ADDED"));

        final ChangelogTaskPartition partition = new ChangelogTaskPartition("key1", state);

        final InputFile inputFile = mock(InputFile.class);
        when(inputFile.getLength()).thenReturn(1024L);
        when(table.schema()).thenReturn(TEST_SCHEMA);
        when(table.io().newInputFile("s3://bucket/file1.parquet")).thenReturn(inputFile);
        when(dataFileReader.open(eq(inputFile), eq(TEST_SCHEMA), eq("s3://bucket/file1.parquet")))
                .thenReturn(CloseableIterable.withNoopClose(List.of(row1, row2)));
        when(tableConfig.getIdentifierColumns()).thenReturn(List.of("id"));
        when(sourceConfig.isAcknowledgmentsEnabled()).thenReturn(false);

        mockCompletionState(100L, 1);

        final BufferAccumulator<org.opensearch.dataprepper.model.record.Record<Event>> bufferAccumulator = mock(BufferAccumulator.class);
        try (MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class)) {
            bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(any(Buffer.class), anyInt(), any(Duration.class)))
                    .thenReturn(bufferAccumulator);

            runWorkerOnce(partition, ChangelogTaskPartition.PARTITION_TYPE);
        }

        verify(bytesProcessedSummary).record(1024L);
        verify(changeEventsProcessedCounter).increment(2);
        verify(exportRecordsProcessedCounter, never()).increment(any(Double.class).doubleValue());
        verify(carryoverRowsRemovedSummary, never()).record(any(Double.class).doubleValue());
    }

    @Test
    void processPartition_withCarryover_recordsCarryoverMetric() throws Exception {
        // Same row appears as DELETED and ADDED = carryover pair
        final Record row = createRecord(1, "Alice");

        final ChangelogTaskProgressState state = new ChangelogTaskProgressState();
        state.setTableName(TABLE_NAME);
        state.setSnapshotId(100L);
        state.setDataFilePaths(List.of("s3://bucket/deleted.parquet", "s3://bucket/added.parquet"));
        state.setTaskTypes(List.of("DELETED", "ADDED"));

        final ChangelogTaskPartition partition = new ChangelogTaskPartition("key1", state);

        final InputFile deletedFile = mock(InputFile.class);
        when(deletedFile.getLength()).thenReturn(512L);
        final InputFile addedFile = mock(InputFile.class);
        when(addedFile.getLength()).thenReturn(512L);

        when(table.schema()).thenReturn(TEST_SCHEMA);
        when(table.io().newInputFile("s3://bucket/deleted.parquet")).thenReturn(deletedFile);
        when(table.io().newInputFile("s3://bucket/added.parquet")).thenReturn(addedFile);
        when(dataFileReader.open(eq(deletedFile), eq(TEST_SCHEMA), eq("s3://bucket/deleted.parquet")))
                .thenReturn(CloseableIterable.withNoopClose(List.of(row)));
        when(dataFileReader.open(eq(addedFile), eq(TEST_SCHEMA), eq("s3://bucket/added.parquet")))
                .thenReturn(CloseableIterable.withNoopClose(List.of(row)));
        when(tableConfig.getIdentifierColumns()).thenReturn(List.of("id"));
        when(sourceConfig.isAcknowledgmentsEnabled()).thenReturn(false);

        mockCompletionState(100L, 1);

        final BufferAccumulator<org.opensearch.dataprepper.model.record.Record<Event>> bufferAccumulator = mock(BufferAccumulator.class);
        try (MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class)) {
            bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(any(Buffer.class), anyInt(), any(Duration.class)))
                    .thenReturn(bufferAccumulator);

            runWorkerOnce(partition, ChangelogTaskPartition.PARTITION_TYPE);
        }

        verify(bytesProcessedSummary, org.mockito.Mockito.times(2)).record(512L);
        verify(carryoverRowsRemovedSummary).record(2);
        verify(changeEventsProcessedCounter).increment(0);
    }

    @Test
    void processInitialLoadPartition_recordsExportMetricsAndBytes() throws Exception {
        final Record row1 = createRecord(1, "Alice");
        final Record row2 = createRecord(2, "Bob");
        final Record row3 = createRecord(3, "Charlie");

        final InitialLoadTaskProgressState state = new InitialLoadTaskProgressState();
        state.setTableName(TABLE_NAME);
        state.setSnapshotId(100L);
        state.setDataFilePath("s3://bucket/export.parquet");

        final InitialLoadTaskPartition partition = new InitialLoadTaskPartition("key1", state);

        final InputFile inputFile = mock(InputFile.class);
        when(inputFile.getLength()).thenReturn(2048L);
        when(table.schema()).thenReturn(TEST_SCHEMA);
        when(table.io().newInputFile("s3://bucket/export.parquet")).thenReturn(inputFile);
        when(dataFileReader.open(eq(inputFile), eq(TEST_SCHEMA), eq("s3://bucket/export.parquet")))
                .thenReturn(CloseableIterable.withNoopClose(List.of(row1, row2, row3)));
        when(tableConfig.getIdentifierColumns()).thenReturn(List.of("id"));
        when(sourceConfig.isAcknowledgmentsEnabled()).thenReturn(false);

        mockCompletionState("initial-100", 1);

        final BufferAccumulator<org.opensearch.dataprepper.model.record.Record<Event>> bufferAccumulator = mock(BufferAccumulator.class);
        try (MockedStatic<BufferAccumulator> bufferAccumulatorMockedStatic = mockStatic(BufferAccumulator.class)) {
            bufferAccumulatorMockedStatic.when(() -> BufferAccumulator.create(any(Buffer.class), anyInt(), any(Duration.class)))
                    .thenReturn(bufferAccumulator);

            runWorkerOnce(partition, InitialLoadTaskPartition.PARTITION_TYPE);
        }

        verify(bytesProcessedSummary).record(2048L);
        verify(exportRecordsProcessedCounter).increment(3);
        verify(changeEventsProcessedCounter, never()).increment(any(Double.class).doubleValue());
    }

    @Test
    void processPartition_error_recordsErrorMetric() throws Exception {
        final ChangelogTaskProgressState state = new ChangelogTaskProgressState();
        state.setTableName(TABLE_NAME);
        state.setSnapshotId(100L);
        state.setDataFilePaths(List.of("s3://bucket/file.parquet"));
        state.setTaskTypes(List.of("ADDED"));

        final ChangelogTaskPartition partition = new ChangelogTaskPartition("key1", state);

        when(table.schema()).thenReturn(TEST_SCHEMA);
        when(table.io().newInputFile("s3://bucket/file.parquet")).thenThrow(new RuntimeException("S3 error"));

        runWorkerOnce(partition, ChangelogTaskPartition.PARTITION_TYPE);

        verify(changeEventsProcessingErrorCounter).increment();
    }

    @Test
    void processInitialLoadPartition_error_recordsErrorMetric() throws Exception {
        final InitialLoadTaskProgressState state = new InitialLoadTaskProgressState();
        state.setTableName(TABLE_NAME);
        state.setSnapshotId(100L);
        state.setDataFilePath("s3://bucket/file.parquet");

        final InitialLoadTaskPartition partition = new InitialLoadTaskPartition("key1", state);

        when(table.schema()).thenReturn(TEST_SCHEMA);
        when(table.io().newInputFile("s3://bucket/file.parquet")).thenThrow(new RuntimeException("S3 error"));

        runWorkerOnce(partition, InitialLoadTaskPartition.PARTITION_TYPE);

        verify(exportRecordsProcessingErrorCounter).increment();
    }

    private Record createRecord(final int id, final String name) {
        final Record record = GenericRecord.create(TEST_SCHEMA);
        record.setField("id", id);
        record.setField("name", name);
        return record;
    }

    private void mockCompletionState(final long snapshotId, final int total) {
        mockCompletionState(String.valueOf(snapshotId), total);
    }

    private void mockCompletionState(final String snapshotKey, final int total) {
        final GlobalState gs = mock(GlobalState.class);
        when(gs.getProgressState()).thenReturn(Optional.of(new java.util.HashMap<>(Map.of("total", total, "completed", 0))));
        when(sourceCoordinator.getPartition("snapshot-completion-" + snapshotKey)).thenReturn(Optional.of(gs));
    }

    private void runWorkerOnce(final EnhancedSourcePartition partition, final String partitionType) {
        // First call returns the partition, second call returns empty (worker sleeps, then gets interrupted)
        when(sourceCoordinator.acquireAvailablePartition(partitionType))
                .thenReturn(Optional.of(partition))
                .thenReturn(Optional.empty());
        if (partitionType.equals(InitialLoadTaskPartition.PARTITION_TYPE)) {
            when(sourceCoordinator.acquireAvailablePartition(ChangelogTaskPartition.PARTITION_TYPE))
                    .thenReturn(Optional.empty());
        }

        final ChangelogWorker worker = new ChangelogWorker(
                sourceCoordinator, sourceConfig,
                Map.of(TABLE_NAME, table), Map.of(TABLE_NAME, tableConfig),
                buffer, acknowledgementSetManager, pluginMetrics, dataFileReader);

        final Thread thread = new Thread(worker);
        thread.start();
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            // ignore
        }
        thread.interrupt();
        try {
            thread.join(2000);
        } catch (InterruptedException e) {
            // ignore
        }
    }
}
