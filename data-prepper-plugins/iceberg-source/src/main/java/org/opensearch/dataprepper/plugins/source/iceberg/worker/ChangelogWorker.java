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

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ChangelogWorker implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ChangelogWorker.class);
    private static final int DEFAULT_LEASE_INTERVAL_MILLIS = 2000;
    private static final int BUFFER_ACCUMULATOR_SIZE = 100;
    private static final Duration BUFFER_TIMEOUT = Duration.ofSeconds(10);

    private final EnhancedSourceCoordinator sourceCoordinator;
    private final IcebergSourceConfig sourceConfig;
    private final Map<String, Table> tables;
    private final Map<String, TableConfig> tableConfigs;
    private final Buffer<org.opensearch.dataprepper.model.record.Record<Event>> buffer;
    private final AcknowledgementSetManager acknowledgementSetManager;

    public ChangelogWorker(final EnhancedSourceCoordinator sourceCoordinator,
                           final IcebergSourceConfig sourceConfig,
                           final Map<String, Table> tables,
                           final Map<String, TableConfig> tableConfigs,
                           final Buffer<org.opensearch.dataprepper.model.record.Record<Event>> buffer,
                           final AcknowledgementSetManager acknowledgementSetManager) {
        this.sourceCoordinator = sourceCoordinator;
        this.sourceConfig = sourceConfig;
        this.tables = tables;
        this.tableConfigs = tableConfigs;
        this.buffer = buffer;
        this.acknowledgementSetManager = acknowledgementSetManager;
    }

    @Override
    public void run() {
        LOG.info("Starting Changelog Worker");

        while (!Thread.currentThread().isInterrupted()) {
            boolean processed = false;
            try {
                // Try changelog tasks first, then initial load tasks
                Optional<EnhancedSourcePartition> partition =
                        sourceCoordinator.acquireAvailablePartition(ChangelogTaskPartition.PARTITION_TYPE);

                if (partition.isPresent() && partition.get() instanceof ChangelogTaskPartition) {
                    processPartition((ChangelogTaskPartition) partition.get());
                    processed = true;
                } else {
                    partition.ifPresent(sourceCoordinator::giveUpPartition);
                    // Try initial load tasks
                    partition = sourceCoordinator.acquireAvailablePartition(
                            InitialLoadTaskPartition.PARTITION_TYPE);
                    if (partition.isPresent() && partition.get() instanceof InitialLoadTaskPartition) {
                        processInitialLoadPartition((InitialLoadTaskPartition) partition.get());
                        processed = true;
                    } else {
                        partition.ifPresent(sourceCoordinator::giveUpPartition);
                    }
                }
            } catch (final Exception e) {
                LOG.error("Error in changelog worker", e);
            }

            if (!processed) {
                try {
                    Thread.sleep(DEFAULT_LEASE_INTERVAL_MILLIS);
                } catch (final InterruptedException e) {
                    LOG.info("Changelog worker interrupted");
                    break;
                }
            }
        }

        LOG.warn("Quitting Changelog Worker");
    }

    private void processPartition(final ChangelogTaskPartition partition) throws Exception {
        final ChangelogTaskProgressState state = partition.getProgressState().orElseThrow();
        final String tableName = state.getTableName();
        final Table table = tables.get(tableName);
        final TableConfig tableConfig = tableConfigs.get(tableName);

        if (table == null || tableConfig == null) {
            LOG.error("Table {} not found, giving up partition", tableName);
            sourceCoordinator.giveUpPartition(partition);
            return;
        }

        final Schema schema = table.schema();
        final ChangelogRecordConverter converter = new ChangelogRecordConverter(
                tableName, tableConfig.getIdentifierColumns());
        final CarryoverRemover carryoverRemover = new CarryoverRemover();

        LOG.info("Processing partition for table {} snapshot {} with {} file(s)",
                tableName, state.getSnapshotId(), state.getDataFilePaths().size());

        // Step 1: Read all rows from all data files in this partition
        final List<RowWithMeta> allRows = new ArrayList<>();
        for (int i = 0; i < state.getDataFilePaths().size(); i++) {
            final String filePath = state.getDataFilePaths().get(i);
            final String taskType = state.getTaskTypes().get(i);
            final String operation = "DELETED".equals(taskType) ? "DELETE" : "INSERT";

            LOG.info("Reading file {} (type: {}, operation: {})", filePath, taskType, operation);

            final InputFile inputFile = table.io().newInputFile(filePath);
            try (CloseableIterable<Record> reader = openDataFile(inputFile, schema, filePath)) {
                for (final Record record : reader) {
                    allRows.add(new RowWithMeta(record, operation));
                    LOG.debug("  Row: {} op={}", record, operation);
                }
            }
        }

        // Step 2: Remove carryover
        final boolean hasDeletedFiles = state.getTaskTypes().stream().anyMatch("DELETED"::equals);
        final List<Integer> survivingIndices;
        if (hasDeletedFiles) {
            final List<CarryoverRemover.ChangelogRow> changelogRows = new ArrayList<>();
            for (int i = 0; i < allRows.size(); i++) {
                final RowWithMeta row = allRows.get(i);
                final List<Object> dataColumns = new ArrayList<>();
                for (final Types.NestedField field : schema.columns()) {
                    dataColumns.add(row.record.getField(field.name()));
                }
                changelogRows.add(new CarryoverRemover.ChangelogRow(dataColumns, row.operation, i));
            }
            survivingIndices = carryoverRemover.removeCarryover(changelogRows);
            LOG.info("Carryover removal: {} rows -> {} rows", allRows.size(), survivingIndices.size());
            for (final int idx : survivingIndices) {
                final RowWithMeta row = allRows.get(idx);
                LOG.debug("  Surviving row: {} op={}", row.record, row.operation);
            }
        } else {
            survivingIndices = new ArrayList<>();
            for (int i = 0; i < allRows.size(); i++) {
                survivingIndices.add(i);
            }
        }

        // Step 3: Convert to events and write to buffer
        // Order: DELETE before INSERT for the same document_id
        final boolean ackEnabled = sourceConfig.isAcknowledgmentsEnabled();
        AcknowledgementSet acknowledgementSet = null;
        if (ackEnabled) {
            acknowledgementSet = acknowledgementSetManager.create((result) -> {
                if (result) {
                    LOG.info("Acknowledgement received for partition {}", partition.getPartitionKey());
                    sourceCoordinator.completePartition(partition);
                } else {
                    LOG.warn("Negative acknowledgement for partition {}, giving up", partition.getPartitionKey());
                    sourceCoordinator.giveUpPartition(partition);
                }
            }, Duration.ofMinutes(30));
        }

        final BufferAccumulator<org.opensearch.dataprepper.model.record.Record<Event>> accumulator =
                BufferAccumulator.create(buffer, BUFFER_ACCUMULATOR_SIZE, BUFFER_TIMEOUT);

        // Write DELETEs first, then INSERTs
        for (final int idx : survivingIndices) {
            final RowWithMeta row = allRows.get(idx);
            if ("DELETE".equals(row.operation)) {
                final Event event = converter.convert(row.record, schema, row.operation, state.getSnapshotId());
                LOG.debug("Writing DELETE event: document_id={}, bulk_action={}",
                        event.getMetadata().getAttribute("document_id"),
                        event.getMetadata().getAttribute("bulk_action"));
                if (acknowledgementSet != null) {
                    acknowledgementSet.add(event);
                }
                accumulator.add(new org.opensearch.dataprepper.model.record.Record<>(event));
            }
        }
        for (final int idx : survivingIndices) {
            final RowWithMeta row = allRows.get(idx);
            if ("INSERT".equals(row.operation)) {
                final Event event = converter.convert(row.record, schema, row.operation, state.getSnapshotId());
                LOG.debug("Writing INSERT event: document_id={}, bulk_action={}",
                        event.getMetadata().getAttribute("document_id"),
                        event.getMetadata().getAttribute("bulk_action"));
                if (acknowledgementSet != null) {
                    acknowledgementSet.add(event);
                }
                accumulator.add(new org.opensearch.dataprepper.model.record.Record<>(event));
            }
        }

        accumulator.flush();

        if (!ackEnabled) {
            sourceCoordinator.completePartition(partition);
            incrementSnapshotCompletionCount(state.getSnapshotId());
        }

        LOG.info("Completed processing partition for table {} snapshot {}: {} events written",
                tableName, state.getSnapshotId(), survivingIndices.size());
    }

    private void processInitialLoadPartition(final InitialLoadTaskPartition partition) throws Exception {
        final InitialLoadTaskProgressState state = partition.getProgressState().orElseThrow();
        final String tableName = state.getTableName();
        final Table table = tables.get(tableName);
        final TableConfig tableConfig = tableConfigs.get(tableName);

        if (table == null || tableConfig == null) {
            LOG.error("Table {} not found for initial load, giving up partition", tableName);
            sourceCoordinator.giveUpPartition(partition);
            return;
        }

        final Schema schema = table.schema();
        final ChangelogRecordConverter converter = new ChangelogRecordConverter(
                tableName, tableConfig.getIdentifierColumns());

        LOG.info("Processing initial load partition for table {} file {}",
                tableName, state.getDataFilePath());

        final InputFile inputFile = table.io().newInputFile(state.getDataFilePath());

        final boolean ackEnabled = sourceConfig.isAcknowledgmentsEnabled();
        AcknowledgementSet acknowledgementSet = null;
        if (ackEnabled) {
            acknowledgementSet = acknowledgementSetManager.create((result) -> {
                if (result) {
                    LOG.info("Acknowledgement received for initial load partition {}", partition.getPartitionKey());
                    sourceCoordinator.completePartition(partition);
                    incrementSnapshotCompletionCount("initial-" + state.getSnapshotId());
                } else {
                    LOG.warn("Negative acknowledgement for initial load partition {}, giving up", partition.getPartitionKey());
                    sourceCoordinator.giveUpPartition(partition);
                }
            }, Duration.ofMinutes(30));
        }

        final BufferAccumulator<org.opensearch.dataprepper.model.record.Record<Event>> accumulator =
                BufferAccumulator.create(buffer, BUFFER_ACCUMULATOR_SIZE, BUFFER_TIMEOUT);

        int rowCount = 0;
        try (CloseableIterable<Record> reader = openDataFile(inputFile, schema, state.getDataFilePath())) {
            for (final Record record : reader) {
                final Event event = converter.convert(record, schema, "INSERT", state.getSnapshotId());
                if (acknowledgementSet != null) {
                    acknowledgementSet.add(event);
                }
                accumulator.add(new org.opensearch.dataprepper.model.record.Record<>(event));
                rowCount++;
            }
        }

        accumulator.flush();

        if (!ackEnabled) {
            sourceCoordinator.completePartition(partition);
            incrementSnapshotCompletionCount("initial-" + state.getSnapshotId());
        }

        LOG.info("Completed initial load partition for table {}: {} rows", tableName, rowCount);
    }

    private void incrementSnapshotCompletionCount(final long snapshotId) {
        incrementSnapshotCompletionCount(String.valueOf(snapshotId));
    }

    private void incrementSnapshotCompletionCount(final String snapshotKey) {
        final String completionKey = "snapshot-completion-" + snapshotKey;
        while (true) {
            final Optional<EnhancedSourcePartition> partitionOpt = sourceCoordinator.getPartition(completionKey);
            if (partitionOpt.isEmpty()) {
                LOG.error("Failed to get completion status for {}", completionKey);
                return;
            }
            final GlobalState gs = (GlobalState) partitionOpt.get();
            final Map<String, Object> progress = new java.util.HashMap<>(
                    gs.getProgressState().orElse(Map.of()));
            final int completed = ((Number) progress.getOrDefault("completed", 0)).intValue();
            progress.put("completed", completed + 1);
            gs.setProgressState(progress);
            try {
                sourceCoordinator.saveProgressStateForPartition(gs, Duration.ZERO);
                break;
            } catch (final Exception e) {
                LOG.warn("Completion count update conflict for {}, retrying", completionKey);
            }
        }
    }

    // TODO: Replace format switch with FormatModelRegistry when available (Iceberg 1.11+).
    // TODO: Add GenericDeleteFilter for MoR support. See GenericReader.open(FileScanTask)
    // in iceberg-data for the reference pattern (delete file merge + format-agnostic reading).
    private CloseableIterable<Record> openDataFile(final InputFile inputFile,
                                                    final Schema schema,
                                                    final String filePath) {
        final FileFormat format = FileFormat.fromFileName(filePath);
        if (format == null) {
            throw new IllegalArgumentException("Cannot determine file format for: " + filePath);
        }
        switch (format) {
            case PARQUET:
                return Parquet.read(inputFile)
                        .project(schema)
                        .createReaderFunc(fs -> GenericParquetReaders.buildReader(schema, fs))
                        .build();
            case AVRO:
                return Avro.read(inputFile)
                        .project(schema)
                        .createReaderFunc(DataReader::create)
                        .build();
            case ORC:
                return ORC.read(inputFile)
                        .project(schema)
                        .createReaderFunc(fs -> GenericOrcReader.buildReader(schema, fs))
                        .build();
            default:
                throw new UnsupportedOperationException("Unsupported file format: " + format);
        }
    }

    private static class RowWithMeta {
        final Record record;
        final String operation;

        RowWithMeta(final Record record, final String operation) {
            this.record = record;
            this.operation = operation;
        }
    }
}
