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
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Types;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.iceberg.IcebergSourceConfig;
import org.opensearch.dataprepper.plugins.source.iceberg.TableConfig;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.partition.ChangelogTaskPartition;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.partition.InitialLoadTaskPartition;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.partition.ShuffleReadPartition;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.partition.ShuffleWritePartition;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.state.ChangelogTaskProgressState;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.state.InitialLoadTaskProgressState;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.state.ShuffleReadProgressState;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.state.ShuffleWriteProgressState;
import org.opensearch.dataprepper.plugins.source.iceberg.leader.LeaderScheduler;
import org.opensearch.dataprepper.plugins.source.iceberg.shuffle.LocalDiskShuffleReader;
import org.opensearch.dataprepper.plugins.source.iceberg.shuffle.ShuffleNodeClient;
import org.opensearch.dataprepper.plugins.source.iceberg.shuffle.ShuffleRecord;
import org.opensearch.dataprepper.plugins.source.iceberg.shuffle.ShuffleStorage;
import org.opensearch.dataprepper.plugins.source.iceberg.shuffle.ShuffleWriter;
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ChangelogWorker implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ChangelogWorker.class);
    private static final int DEFAULT_LEASE_INTERVAL_MILLIS = 2000;
    private static final int BUFFER_ACCUMULATOR_SIZE = 100;
    private static final Duration BUFFER_TIMEOUT = Duration.ofSeconds(10);

    static final String CHANGE_EVENTS_PROCESSED_COUNT = "changeEventsProcessed";
    static final String CHANGE_EVENTS_PROCESSING_ERROR_COUNT = "changeEventsProcessingErrors";
    static final String EXPORT_RECORDS_PROCESSED_COUNT = "exportRecordsProcessed";
    static final String EXPORT_RECORDS_PROCESSING_ERROR_COUNT = "exportRecordsProcessingErrors";
    static final String BYTES_PROCESSED = "bytesProcessed";
    static final String CARRYOVER_ROWS_REMOVED = "carryoverRowsRemoved";
    static final String SHUFFLE_RECORDS_WRITTEN_COUNT = "shuffleRecordsWritten";

    private final EnhancedSourceCoordinator sourceCoordinator;
    private final IcebergSourceConfig sourceConfig;
    private final Map<String, Table> tables;
    private final Map<String, TableConfig> tableConfigs;
    private final Buffer<org.opensearch.dataprepper.model.record.Record<Event>> buffer;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final EventFactory eventFactory;
    private final ShuffleStorage shuffleStorage;
    private final ShuffleNodeClient shuffleNodeClient;
    private final String localNodeAddress;
    private final IcebergDataFileReader dataFileReader;
    private final Counter changeEventsProcessedCounter;
    private final Counter changeEventsProcessingErrorCounter;
    private final Counter exportRecordsProcessedCounter;
    private final Counter exportRecordsProcessingErrorCounter;
    private final DistributionSummary bytesProcessedSummary;
    private final DistributionSummary carryoverRowsRemovedSummary;
    private final Counter shuffleRecordsWrittenCounter;

    public ChangelogWorker(final EnhancedSourceCoordinator sourceCoordinator,
                           final IcebergSourceConfig sourceConfig,
                           final Map<String, Table> tables,
                           final Map<String, TableConfig> tableConfigs,
                           final Buffer<org.opensearch.dataprepper.model.record.Record<Event>> buffer,
                           final AcknowledgementSetManager acknowledgementSetManager,
                           final EventFactory eventFactory,
                           final ShuffleStorage shuffleStorage,
                           final Certificate certificate,
                           final PluginMetrics pluginMetrics,
                           final IcebergDataFileReader dataFileReader) {
        this.sourceCoordinator = sourceCoordinator;
        this.sourceConfig = sourceConfig;
        this.tables = tables;
        this.tableConfigs = tableConfigs;
        this.buffer = buffer;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.eventFactory = eventFactory;
        this.shuffleStorage = shuffleStorage;
        this.shuffleNodeClient = new ShuffleNodeClient(sourceConfig.getShuffleConfig(), certificate);
        this.localNodeAddress = ShuffleNodeClient.resolveLocalAddress();
        this.dataFileReader = dataFileReader;
        this.changeEventsProcessedCounter = pluginMetrics.counter(CHANGE_EVENTS_PROCESSED_COUNT);
        this.changeEventsProcessingErrorCounter = pluginMetrics.counter(CHANGE_EVENTS_PROCESSING_ERROR_COUNT);
        this.exportRecordsProcessedCounter = pluginMetrics.counter(EXPORT_RECORDS_PROCESSED_COUNT);
        this.exportRecordsProcessingErrorCounter = pluginMetrics.counter(EXPORT_RECORDS_PROCESSING_ERROR_COUNT);
        this.bytesProcessedSummary = pluginMetrics.summary(BYTES_PROCESSED);
        this.carryoverRowsRemovedSummary = pluginMetrics.summary(CARRYOVER_ROWS_REMOVED);
        this.shuffleRecordsWrittenCounter = pluginMetrics.counter(SHUFFLE_RECORDS_WRITTEN_COUNT);
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
                    try {
                        processPartition((ChangelogTaskPartition) partition.get());
                    } catch (final Exception e) {
                        changeEventsProcessingErrorCounter.increment();
                        throw e;
                    }
                    processed = true;
                } else {
                    partition.ifPresent(sourceCoordinator::giveUpPartition);
                    // Try shuffle write tasks
                    partition = sourceCoordinator.acquireAvailablePartition(ShuffleWritePartition.PARTITION_TYPE);
                    if (partition.isPresent() && partition.get() instanceof ShuffleWritePartition) {
                        try {
                            processShuffleWrite((ShuffleWritePartition) partition.get());
                        } catch (final Exception e) {
                            changeEventsProcessingErrorCounter.increment();
                            throw e;
                        }
                        processed = true;
                    } else {
                        partition.ifPresent(sourceCoordinator::giveUpPartition);
                        // Try shuffle read tasks
                        partition = sourceCoordinator.acquireAvailablePartition(ShuffleReadPartition.PARTITION_TYPE);
                        if (partition.isPresent() && partition.get() instanceof ShuffleReadPartition) {
                            processShuffleRead((ShuffleReadPartition) partition.get());
                            processed = true;
                        } else {
                            partition.ifPresent(sourceCoordinator::giveUpPartition);
                            // Try initial load tasks
                            partition = sourceCoordinator.acquireAvailablePartition(
                                    InitialLoadTaskPartition.PARTITION_TYPE);
                            if (partition.isPresent() && partition.get() instanceof InitialLoadTaskPartition) {
                                try {
                                    processInitialLoadPartition((InitialLoadTaskPartition) partition.get());
                                } catch (final Exception e) {
                                    exportRecordsProcessingErrorCounter.increment();
                                    throw e;
                                }
                                processed = true;
                            } else {
                                partition.ifPresent(sourceCoordinator::giveUpPartition);
                            }
                        }
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
                tableName, tableConfig.getIdentifierColumns(), eventFactory);
        final CarryoverRemover carryoverRemover = new CarryoverRemover();

        LOG.debug("Processing partition for table {} snapshot {} with {} file(s)",
                tableName, state.getSnapshotId(), state.getDataFilePaths().size());

        // Step 1: Read all rows from all data files in this partition
        final List<RowWithMeta> allRows = new ArrayList<>();
        for (int i = 0; i < state.getDataFilePaths().size(); i++) {
            final String filePath = state.getDataFilePaths().get(i);
            final String taskType = state.getTaskTypes().get(i);
            final String operation = "DELETED".equals(taskType) ? "DELETE" : "INSERT";

            LOG.debug("Reading file {} (type: {}, operation: {})", filePath, taskType, operation);

            final InputFile inputFile = table.io().newInputFile(filePath);
            bytesProcessedSummary.record(inputFile.getLength());
            try (CloseableIterable<Record> reader = dataFileReader.open(inputFile, schema, filePath)) {
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
            final int removedCount = allRows.size() - survivingIndices.size();
            LOG.debug("Carryover removal: {} rows -> {} rows", allRows.size(), survivingIndices.size());
            carryoverRowsRemovedSummary.record(removedCount);
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

        // Step 3: Merge UPDATE pairs and write to buffer
        // When identifier_columns is set, a DELETE + INSERT pair with the same document_id
        // represents an UPDATE. Since OpenSearch INDEX is an upsert, the INSERT alone
        // overwrites the existing document, making the DELETE unnecessary.
        final boolean ackEnabled = sourceConfig.isAcknowledgmentsEnabled();
        AcknowledgementSet acknowledgementSet = null;
        if (ackEnabled) {
            acknowledgementSet = acknowledgementSetManager.create((result) -> {
                try {
                    if (result) {
                        LOG.info("Acknowledgement received for partition {}", partition.getPartitionKey());
                        sourceCoordinator.completePartition(partition);
                        incrementSnapshotCompletionCount(state.getSnapshotId());
                    } else {
                        LOG.warn("Negative acknowledgement for partition {}, giving up", partition.getPartitionKey());
                        sourceCoordinator.giveUpPartition(partition);
                    }
                } catch (final Exception e) {
                    LOG.error("Error in acknowledgement callback for partition {}", partition.getPartitionKey(), e);
                }
            }, Duration.ofMinutes(30));
        }

        final BufferAccumulator<org.opensearch.dataprepper.model.record.Record<Event>> accumulator =
                BufferAccumulator.create(buffer, BUFFER_ACCUMULATOR_SIZE, BUFFER_TIMEOUT);

        // Identify DELETE indices whose document_id has a matching INSERT (i.e., UPDATE pairs)
        final Set<Integer> deletesToSkip = new HashSet<>();
        if (!tableConfig.getIdentifierColumns().isEmpty()) {
            final Map<String, Integer> deleteByDocId = new LinkedHashMap<>();
            for (final int idx : survivingIndices) {
                final RowWithMeta row = allRows.get(idx);
                if ("DELETE".equals(row.operation)) {
                    final String docId = buildDocumentId(row.record, schema, tableConfig.getIdentifierColumns());
                    deleteByDocId.put(docId, idx);
                }
            }
            for (final int idx : survivingIndices) {
                final RowWithMeta row = allRows.get(idx);
                if ("INSERT".equals(row.operation)) {
                    final String docId = buildDocumentId(row.record, schema, tableConfig.getIdentifierColumns());
                    if (deleteByDocId.containsKey(docId)) {
                        deletesToSkip.add(deleteByDocId.remove(docId));
                    }
                }
            }
            if (!deletesToSkip.isEmpty()) {
                LOG.debug("Merged {} UPDATE pair(s) (DELETE + INSERT -> INDEX only)", deletesToSkip.size());
            }
        }

        for (final int idx : survivingIndices) {
            if (deletesToSkip.contains(idx)) {
                continue;
            }
            final RowWithMeta row = allRows.get(idx);
            final Event event = converter.convert(row.record, schema, row.operation, state.getSnapshotId());
            LOG.debug("Writing event: op={}, document_id={}, bulk_action={}",
                    row.operation,
                    event.getMetadata().getAttribute("document_id"),
                    event.getMetadata().getAttribute("bulk_action"));
            if (acknowledgementSet != null) {
                acknowledgementSet.add(event);
            }
            accumulator.add(new org.opensearch.dataprepper.model.record.Record<>(event));
        }

        accumulator.flush();

        if (ackEnabled) {
            if (survivingIndices.isEmpty()) {
                sourceCoordinator.completePartition(partition);
                incrementSnapshotCompletionCount(state.getSnapshotId());
            } else {
                acknowledgementSet.complete();
            }
        } else {
            sourceCoordinator.completePartition(partition);
            incrementSnapshotCompletionCount(state.getSnapshotId());
        }

        LOG.debug("Completed processing partition for table {} snapshot {}: {} events written",
                tableName, state.getSnapshotId(), survivingIndices.size() - deletesToSkip.size());
        changeEventsProcessedCounter.increment(survivingIndices.size() - deletesToSkip.size());
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
                tableName, tableConfig.getIdentifierColumns(), eventFactory);

        LOG.debug("Processing initial load partition for table {} file {}",
                tableName, state.getDataFilePath());

        final InputFile inputFile = table.io().newInputFile(state.getDataFilePath());
        bytesProcessedSummary.record(inputFile.getLength());

        final boolean ackEnabled = sourceConfig.isAcknowledgmentsEnabled();
        AcknowledgementSet acknowledgementSet = null;
        if (ackEnabled) {
            acknowledgementSet = acknowledgementSetManager.create((result) -> {
                try {
                    if (result) {
                        LOG.info("Acknowledgement received for initial load partition {}", partition.getPartitionKey());
                        sourceCoordinator.completePartition(partition);
                        incrementSnapshotCompletionCount("initial-" + state.getSnapshotId());
                    } else {
                        LOG.warn("Negative acknowledgement for initial load partition {}, giving up", partition.getPartitionKey());
                        sourceCoordinator.giveUpPartition(partition);
                    }
                } catch (final Exception e) {
                    LOG.error("Error in acknowledgement callback for initial load partition {}", partition.getPartitionKey(), e);
                }
            }, Duration.ofMinutes(30));
        }

        final BufferAccumulator<org.opensearch.dataprepper.model.record.Record<Event>> accumulator =
                BufferAccumulator.create(buffer, BUFFER_ACCUMULATOR_SIZE, BUFFER_TIMEOUT);

        int rowCount = 0;
        try (CloseableIterable<Record> reader = dataFileReader.open(inputFile, schema, state.getDataFilePath())) {
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

        if (ackEnabled) {
            acknowledgementSet.complete();
        } else {
            sourceCoordinator.completePartition(partition);
            incrementSnapshotCompletionCount("initial-" + state.getSnapshotId());
        }

        LOG.debug("Completed initial load partition for table {}: {} rows", tableName, rowCount);
        exportRecordsProcessedCounter.increment(rowCount);
    }

    private void incrementSnapshotCompletionCount(final long snapshotId) {
        incrementSnapshotCompletionCount(String.valueOf(snapshotId));
    }

    private String buildDocumentId(final Record record, final Schema schema, final List<String> identifierColumns) {
        return identifierColumns.stream()
                .map(col -> String.valueOf(record.getField(col)))
                .collect(java.util.stream.Collectors.joining("|"));
    }

    private synchronized void incrementSnapshotCompletionCount(final String snapshotKey) {
        final String completionKey = "snapshot-completion-" + snapshotKey;
        final int maxRetries = 10;
        for (int attempt = 0; attempt < maxRetries; attempt++) {
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
                return;
            } catch (final Exception e) {
                LOG.warn("Completion count update conflict for {}, attempt {}/{}", completionKey, attempt + 1, maxRetries);
            }
        }
        throw new RuntimeException("Failed to update completion count for " + completionKey + " after " + maxRetries + " attempts");
    }

    private static class RowWithMeta {
        final Record record;
        final String operation;

        RowWithMeta(final Record record, final String operation) {
            this.record = record;
            this.operation = operation;
        }
    }

    private void processShuffleWrite(final ShuffleWritePartition partition) throws Exception {
        final ShuffleWriteProgressState state = partition.getProgressState().orElseThrow();
        final String tableName = state.getTableName();
        final Table table = tables.get(tableName);

        if (table == null) {
            LOG.error("Table {} not found for shuffle write, giving up", tableName);
            sourceCoordinator.giveUpPartition(partition);
            return;
        }

        final Schema schema = table.schema();
        final TableConfig tableConfig = tableConfigs.get(tableName);
        final List<String> identifierColumns = tableConfig.getIdentifierColumns();
        final int numPartitions = sourceConfig.getShuffleConfig().getPartitions();
        final String snapshotIdStr = String.valueOf(state.getSnapshotId());
        final String operation = "DELETED".equals(state.getTaskType()) ? "DELETE" : "INSERT";

        LOG.debug("SHUFFLE_WRITE: table={} file={} type={}", tableName, state.getDataFilePath(), state.getTaskType());

        // Record node address for SHUFFLE_READ to know where to pull from
        state.setNodeAddress(localNodeAddress);

        final InputFile inputFile = table.io().newInputFile(state.getDataFilePath());
        bytesProcessedSummary.record(inputFile.getLength());
        try (ShuffleWriter writer = shuffleStorage.createWriter(snapshotIdStr, state.getShuffleTaskId(), numPartitions);
             CloseableIterable<Record> reader = dataFileReader.open(inputFile, schema, state.getDataFilePath())) {

            final org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(schema, tableName);

            int recordCount = 0;
            for (final Record record : reader) {
                final int partitionNum = computeShufflePartition(record, identifierColumns, numPartitions);
                final byte op = "DELETE".equals(operation) ? ShuffleRecord.OP_DELETE : ShuffleRecord.OP_INSERT;
                final byte[] serialized = RecordAvroSerializer.serialize(record, avroSchema);
                writer.addRecord(partitionNum, op, state.getChangeOrdinal(), serialized);
                recordCount++;
            }
            writer.finish();
            shuffleRecordsWrittenCounter.increment(recordCount);
        }

        // Register location in GlobalState (Spark MapStatus pattern)
        registerShuffleWriteLocation(state.getSnapshotId(), state.getShuffleTaskId(), localNodeAddress);

        sourceCoordinator.completePartition(partition);
        incrementSnapshotCompletionCount("sw-" + state.getSnapshotId());
        LOG.debug("SHUFFLE_WRITE completed: task={}", state.getShuffleTaskId());
    }

    private void processShuffleRead(final ShuffleReadPartition partition) {
        final ShuffleReadProgressState state = partition.getProgressState().orElseThrow();
        final String tableName = state.getTableName();
        final Table table = tables.get(tableName);
        final TableConfig tableConfig = tableConfigs.get(tableName);

        if (table == null || tableConfig == null) {
            LOG.error("Table {} not found for shuffle read, giving up", tableName);
            sourceCoordinator.giveUpPartition(partition);
            return;
        }

        final String snapshotIdStr = String.valueOf(state.getSnapshotId());
        final int startPartition = state.getPartitionRangeStart();
        final int endPartition = state.getPartitionRangeEnd();

        LOG.debug("SHUFFLE_READ: table={} partitions={}-{}", tableName, startPartition, endPartition);

        try {
            // Collect records from all nodes for our partition range
            final List<ShuffleRecord> allRecords = new ArrayList<>();

            LOG.debug("SHUFFLE_READ taskIds={} nodeAddresses={}", state.getShuffleWriteTaskIds(), state.getNodeAddresses());
            for (int i = 0; i < state.getShuffleWriteTaskIds().size(); i++) {
                final String taskId = state.getShuffleWriteTaskIds().get(i);
                final String nodeAddress = state.getNodeAddresses().get(
                        i < state.getNodeAddresses().size() ? i : 0);

                final List<ShuffleRecord> records = pullShuffleData(
                        snapshotIdStr, taskId, nodeAddress, startPartition, endPartition);
                allRecords.addAll(records);
            }

            if (allRecords.isEmpty()) {
                sourceCoordinator.completePartition(partition);
                incrementSnapshotCompletionCount("sr-" + state.getSnapshotId());
                return;
            }

            // Deserialize, carryover removal, UPDATE merge, write to buffer
            final Schema schema = table.schema();
            final org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(schema, tableName);
            final ChangelogRecordConverter converter = new ChangelogRecordConverter(tableName, tableConfig.getIdentifierColumns(), eventFactory);
            final CarryoverRemover carryoverRemover = new CarryoverRemover();

            // Convert ShuffleRecords to RowWithMeta
            final List<RowWithMeta> rows = new ArrayList<>();
            for (final ShuffleRecord sr : allRecords) {
                final Record record = RecordAvroSerializer.deserialize(sr.getSerializedRecord(), schema, avroSchema);
                final String op = sr.getOperation() == ShuffleRecord.OP_DELETE ? "DELETE" : "INSERT";
                rows.add(new RowWithMeta(record, op));
            }

            // Carryover removal
            final List<CarryoverRemover.ChangelogRow> changelogRows = new ArrayList<>();
            for (int i = 0; i < rows.size(); i++) {
                final RowWithMeta row = rows.get(i);
                final List<Object> dataColumns = new ArrayList<>();
                for (final Types.NestedField field : schema.columns()) {
                    dataColumns.add(row.record.getField(field.name()));
                }
                changelogRows.add(new CarryoverRemover.ChangelogRow(dataColumns, row.operation, i));
            }
            final List<Integer> survivingIndices = carryoverRemover.removeCarryover(changelogRows);
            final int removedCount = rows.size() - survivingIndices.size();
            if (removedCount > 0) {
                LOG.debug("Carryover removal: {} rows -> {} rows", rows.size(), survivingIndices.size());
                carryoverRowsRemovedSummary.record(removedCount);
            }

            // UPDATE merge
            final Set<Integer> deletesToSkip = new HashSet<>();
            if (!tableConfig.getIdentifierColumns().isEmpty()) {
                final Map<String, Integer> deleteByDocId = new LinkedHashMap<>();
                for (final int idx : survivingIndices) {
                    final RowWithMeta row = rows.get(idx);
                    if ("DELETE".equals(row.operation)) {
                        deleteByDocId.put(buildDocumentId(row.record, schema, tableConfig.getIdentifierColumns()), idx);
                    }
                }
                for (final int idx : survivingIndices) {
                    final RowWithMeta row = rows.get(idx);
                    if ("INSERT".equals(row.operation)) {
                        final String docId = buildDocumentId(row.record, schema, tableConfig.getIdentifierColumns());
                        if (deleteByDocId.containsKey(docId)) {
                            deletesToSkip.add(deleteByDocId.remove(docId));
                        }
                    }
                }
            }
            if (!deletesToSkip.isEmpty()) {
                LOG.debug("Merged {} UPDATE pair(s) (DELETE + INSERT -> INDEX only)", deletesToSkip.size());
            }

            // Write to buffer
            final BufferAccumulator<org.opensearch.dataprepper.model.record.Record<Event>> accumulator =
                    BufferAccumulator.create(buffer, BUFFER_ACCUMULATOR_SIZE, BUFFER_TIMEOUT);

            for (final int idx : survivingIndices) {
                if (deletesToSkip.contains(idx)) continue;
                final RowWithMeta row = rows.get(idx);
                final Event event = converter.convert(row.record, schema, row.operation, state.getSnapshotId());
                accumulator.add(new org.opensearch.dataprepper.model.record.Record<>(event));
            }
            accumulator.flush();

            final int eventsWritten = survivingIndices.size() - deletesToSkip.size();
            sourceCoordinator.completePartition(partition);
            incrementSnapshotCompletionCount("sr-" + state.getSnapshotId());
            changeEventsProcessedCounter.increment(eventsWritten);
            LOG.debug("SHUFFLE_READ completed: partitions={}-{}, {} events", startPartition, endPartition, eventsWritten);

        } catch (final Exception e) {
            changeEventsProcessingErrorCounter.increment();
            LOG.error("SHUFFLE_READ failed for partitions {}-{}", startPartition, endPartition, e);
            markShuffleFailed(snapshotIdStr);
            sourceCoordinator.giveUpPartition(partition);
        }
    }

    private List<ShuffleRecord> pullShuffleData(final String snapshotId, final String taskId,
                                                 final String nodeAddress, final int startPartition,
                                                 final int endPartition) throws Exception {
        LOG.debug("Pulling shuffle data: snapshot={} task={} node={} partitions={}-{} isLocal={}",
                snapshotId, taskId, nodeAddress, startPartition, endPartition, ShuffleNodeClient.isLocalAddress(nodeAddress));
        // Local node: read directly from disk
        if (ShuffleNodeClient.isLocalAddress(nodeAddress)) {
            try (var reader = shuffleStorage.createReader(snapshotId, taskId)) {
                return reader.readPartitions(startPartition, endPartition);
            }
        }

        // Remote node: get index, then pull each partition's compressed block
        final long[] offsets = shuffleNodeClient.pullIndex(nodeAddress, snapshotId, taskId);

        final List<ShuffleRecord> allRecords = new ArrayList<>();
        for (int p = startPartition; p <= endPartition; p++) {
            final long offset = offsets[p];
            final long end = offsets[p + 1];
            final int length = (int) (end - offset);
            if (length == 0) {
                continue;
            }

            final byte[] compressedBlock = shuffleNodeClient.pullData(nodeAddress, snapshotId, taskId, offset, length);
            final byte[] uncompressed = LocalDiskShuffleReader.decompressBlock(compressedBlock);
            allRecords.addAll(LocalDiskShuffleReader.parseRecords(uncompressed));
        }
        return allRecords;
    }

    private int computeShufflePartition(final Record record,
                                         final List<String> identifierColumns, final int numPartitions) {
        int hash = 0;
        for (final String col : identifierColumns) {
            final Object val = record.getField(col);
            hash = 31 * hash + (val != null ? val.toString().hashCode() : 0);
        }
        // floorMod always returns a non-negative result, unlike % which can return negative values
        // when the hash is negative (e.g. String.hashCode() can produce negative values).
        return Math.floorMod(hash, numPartitions);
    }

    private synchronized void registerShuffleWriteLocation(final long snapshotId, final String taskId, final String nodeAddress) {
        final String locationKey = "shuffle-locations-" + snapshotId;
        final int maxRetries = 10;
        for (int attempt = 0; attempt < maxRetries; attempt++) {
            final Optional<EnhancedSourcePartition> partitionOpt = sourceCoordinator.getPartition(locationKey);
            if (partitionOpt.isEmpty()) {
                LOG.error("Failed to get shuffle location state for {}", locationKey);
                return;
            }
            final GlobalState gs = (GlobalState) partitionOpt.get();
            final Map<String, Object> locations = new java.util.HashMap<>(gs.getProgressState().orElse(Map.of()));
            locations.put(taskId, nodeAddress);
            gs.setProgressState(locations);
            try {
                sourceCoordinator.saveProgressStateForPartition(gs, Duration.ZERO);
                return;
            } catch (final Exception e) {
                LOG.warn("Location update conflict for {}, attempt {}/{}", locationKey, attempt + 1, maxRetries);
            }
        }
        throw new RuntimeException("Failed to register shuffle write location for " + locationKey + " after " + maxRetries + " attempts");
    }

    private void markShuffleFailed(final String snapshotIdStr) {
        final String key = LeaderScheduler.SHUFFLE_FAILED_PREFIX + snapshotIdStr;
        try {
            sourceCoordinator.createPartition(new GlobalState(key, Map.of("failed", true)));
        } catch (final Exception e) {
            LOG.warn("Failed to mark shuffle as failed for snapshot {}", snapshotIdStr, e);
        }
    }
}
