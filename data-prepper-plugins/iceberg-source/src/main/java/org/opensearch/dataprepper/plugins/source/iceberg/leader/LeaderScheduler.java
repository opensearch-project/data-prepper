/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.iceberg.leader;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.iceberg.TableConfig;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.partition.ChangelogTaskPartition;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.partition.InitialLoadTaskPartition;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.state.ChangelogTaskProgressState;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.state.InitialLoadTaskProgressState;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.state.LeaderProgressState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class LeaderScheduler implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(LeaderScheduler.class);
    private static final Duration DEFAULT_EXTEND_LEASE_DURATION = Duration.ofMinutes(3);
    private static final Duration COMPLETION_CHECK_INTERVAL = Duration.ofSeconds(2);
    static final String SNAPSHOT_COMPLETION_PREFIX = "snapshot-completion-";
    static final String SNAPSHOTS_PROCESSED_COUNT = "snapshotsProcessed";
    static final String DATA_FILE_BYTES = "dataFileBytes";
    static final String DATA_FILES_PER_TASK = "dataFilesPerTask";

    private final EnhancedSourceCoordinator sourceCoordinator;
    private final Map<String, TableConfig> tableConfigs;
    private final Duration pollingInterval;
    private final Map<String, Table> tables;
    private final TaskGrouper taskGrouper = new TaskGrouper();
    private final Counter snapshotsProcessedCounter;
    private final DistributionSummary dataFileBytesSummary;
    private final DistributionSummary dataFilesPerTaskSummary;
    private LeaderPartition leaderPartition;

    public LeaderScheduler(final EnhancedSourceCoordinator sourceCoordinator,
                           final Map<String, TableConfig> tableConfigs,
                           final Duration pollingInterval,
                           final Map<String, Table> tables,
                           final PluginMetrics pluginMetrics) {
        this.sourceCoordinator = sourceCoordinator;
        this.tableConfigs = tableConfigs;
        this.pollingInterval = pollingInterval;
        this.tables = tables;
        this.snapshotsProcessedCounter = pluginMetrics.counter(SNAPSHOTS_PROCESSED_COUNT);
        this.dataFileBytesSummary = pluginMetrics.summary(DATA_FILE_BYTES);
        this.dataFilesPerTaskSummary = pluginMetrics.summary(DATA_FILES_PER_TASK);
    }

    @Override
    public void run() {
        LOG.info("Starting Iceberg Leader Scheduler");

        while (!Thread.currentThread().isInterrupted()) {
            try {
                if (leaderPartition == null) {
                    final Optional<EnhancedSourcePartition> sourcePartition =
                            sourceCoordinator.acquireAvailablePartition(LeaderPartition.PARTITION_TYPE);
                    if (sourcePartition.isPresent()) {
                        LOG.info("Running as LEADER node");
                        leaderPartition = (LeaderPartition) sourcePartition.get();
                    }
                }

                if (leaderPartition != null) {
                    final LeaderProgressState progressState = leaderPartition.getProgressState().orElseThrow();
                    if (!progressState.isInitialized()) {
                        performInitialLoad();
                        progressState.setInitialized(true);
                        LOG.info("Leader initialized");
                    }
                    pollAndPlan();
                }
            } catch (final Exception e) {
                LOG.error("Exception in leader scheduling loop", e);
            } finally {
                if (leaderPartition != null) {
                    try {
                        sourceCoordinator.saveProgressStateForPartition(
                                leaderPartition, DEFAULT_EXTEND_LEASE_DURATION);
                    } catch (final Exception e) {
                        LOG.error("Failed to extend leader lease, will attempt to reacquire");
                        leaderPartition = null;
                    }
                }
                try {
                    Thread.sleep(pollingInterval.toMillis());
                } catch (final InterruptedException e) {
                    LOG.info("Leader scheduler interrupted");
                    break;
                }
            }
        }

        LOG.warn("Quitting Leader Scheduler");
        if (leaderPartition != null) {
            sourceCoordinator.giveUpPartition(leaderPartition);
        }
    }

    private void performInitialLoad() {
        final LeaderProgressState progressState = leaderPartition.getProgressState().orElseThrow();

        for (final Map.Entry<String, Table> entry : tables.entrySet()) {
            final String tableName = entry.getKey();
            final Table table = entry.getValue();
            final TableConfig tableConfig = tableConfigs.get(tableName);

            if (tableConfig.isDisableExport()) {
                LOG.info("Skipping initial Export for table {} (isDisableExport=true)", tableName);
                continue;
            }

            table.refresh();
            if (table.currentSnapshot() == null) {
                LOG.info("No snapshot for table {}, skipping initial load", tableName);
                continue;
            }

            final long snapshotId = table.currentSnapshot().snapshotId();
            LOG.info("Starting initial load for table {} at snapshot {}", tableName, snapshotId);

            final TableScan scan = table.newScan().useSnapshot(snapshotId);
            int taskCount = 0;

            try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
                for (final FileScanTask task : tasks) {
                    dataFileBytesSummary.record(task.file().fileSizeInBytes());
                    dataFilesPerTaskSummary.record(1);

                    final InitialLoadTaskProgressState taskState = new InitialLoadTaskProgressState();
                    taskState.setSnapshotId(snapshotId);
                    taskState.setTableName(tableName);
                    taskState.setDataFilePath(task.file().location());
                    taskState.setTotalRecords(task.file().recordCount());

                    final String partitionKey = tableName + "|initial|" + UUID.randomUUID();
                    sourceCoordinator.createPartition(new InitialLoadTaskPartition(partitionKey, taskState));
                    taskCount++;
                }
            } catch (final java.io.IOException e) {
                throw new RuntimeException("Failed to plan initial load for " + tableName, e);
            }

            LOG.info("Created {} initial load partition(s) for table {} snapshot {}",
                    taskCount, tableName, snapshotId);

            // Wait for all initial load partitions to complete
            final String completionKey = SNAPSHOT_COMPLETION_PREFIX + "initial-" + snapshotId;
            sourceCoordinator.createPartition(new GlobalState(completionKey,
                    Map.of("total", taskCount, "completed", 0)));
            waitForSnapshotComplete(completionKey, taskCount);

            // Set lastProcessedSnapshotId so CDC starts from this snapshot
            progressState.setLastProcessedSnapshotId(snapshotId);
            sourceCoordinator.saveProgressStateForPartition(leaderPartition, DEFAULT_EXTEND_LEASE_DURATION);

            LOG.info("Initial load completed for table {} at snapshot {}", tableName, snapshotId);
        }
    }

    private void pollAndPlan() {
        final LeaderProgressState progressState = leaderPartition.getProgressState().orElseThrow();

        for (final Map.Entry<String, Table> entry : tables.entrySet()) {
            final String tableName = entry.getKey();
            final Table table = entry.getValue();

            table.refresh();

            if (table.currentSnapshot() == null) {
                continue;
            }

            final long currentSnapshotId = table.currentSnapshot().snapshotId();
            final Long lastProcessedId = progressState.getLastProcessedSnapshotId();

            if (lastProcessedId != null && lastProcessedId.equals(currentSnapshotId)) {
                continue;
            }

            final List<Snapshot> snapshotsToProcess = getSnapshotsBetween(
                    table, lastProcessedId, currentSnapshotId);

            if (snapshotsToProcess.isEmpty()) {
                progressState.setLastProcessedSnapshotId(currentSnapshotId);
                continue;
            }

            LOG.info("Processing {} snapshot(s) for table {} ({} -> {})",
                    snapshotsToProcess.size(), tableName, lastProcessedId, currentSnapshotId);

            for (final Snapshot snapshot : snapshotsToProcess) {
                if (Thread.currentThread().isInterrupted()) {
                    break;
                }

                final long parentId = snapshot.parentId() != null ? snapshot.parentId() : -1;
                if (parentId == -1) {
                    LOG.info("First snapshot {} for {}, skipping (use initial_load)",
                            snapshot.snapshotId(), tableName);
                    progressState.setLastProcessedSnapshotId(snapshot.snapshotId());
                    continue;
                }

                if ("replace".equals(snapshot.operation())) {
                    LOG.debug("Skipping REPLACE snapshot {} for {}",
                            snapshot.snapshotId(), tableName);
                    progressState.setLastProcessedSnapshotId(snapshot.snapshotId());
                    continue;
                }

                LOG.info("Planning snapshot {} for table {} (operation: {})",
                        snapshot.snapshotId(), tableName, snapshot.operation());

                final List<ChangelogTaskProgressState> taskGroups =
                        taskGrouper.planAndGroup(table, tableName, parentId, snapshot.snapshotId(),
                                dataFileBytesSummary, dataFilesPerTaskSummary);

                if (taskGroups.isEmpty()) {
                    progressState.setLastProcessedSnapshotId(snapshot.snapshotId());
                    continue;
                }

                // Create a completion tracker in GlobalState
                final String completionKey = SNAPSHOT_COMPLETION_PREFIX + snapshot.snapshotId();
                sourceCoordinator.createPartition(new GlobalState(completionKey,
                        Map.of("total", taskGroups.size(), "completed", 0)));

                for (final ChangelogTaskProgressState taskState : taskGroups) {
                    final String partitionKey = tableName + "|" + snapshot.snapshotId()
                            + "|" + UUID.randomUUID();
                    sourceCoordinator.createPartition(
                            new ChangelogTaskPartition(partitionKey, taskState));
                }

                LOG.info("Created {} partition(s) for snapshot {}, waiting for completion",
                        taskGroups.size(), snapshot.snapshotId());

                waitForSnapshotComplete(completionKey, taskGroups.size());

                progressState.setLastProcessedSnapshotId(snapshot.snapshotId());
                sourceCoordinator.saveProgressStateForPartition(
                        leaderPartition, DEFAULT_EXTEND_LEASE_DURATION);

                LOG.info("Snapshot {} completed for table {}", snapshot.snapshotId(), tableName);
                snapshotsProcessedCounter.increment();
            }
        }
    }

    private List<Snapshot> getSnapshotsBetween(final Table table,
                                                final Long fromExclusive,
                                                final long toInclusive) {
        final List<Snapshot> result = new ArrayList<>();
        Snapshot current = table.snapshot(toInclusive);
        while (current != null) {
            if (fromExclusive != null && fromExclusive.equals(current.snapshotId())) {
                break;
            }
            result.add(0, current);
            if (current.parentId() == null) {
                break;
            }
            current = table.snapshot(current.parentId());
        }
        return result;
    }

    private void waitForSnapshotComplete(final String completionKey, final int totalPartitions) {
        while (!Thread.currentThread().isInterrupted()) {
            final Optional<EnhancedSourcePartition> state =
                    sourceCoordinator.getPartition(completionKey);

            if (state.isPresent()) {
                final GlobalState gs = (GlobalState) state.get();
                final Map<String, Object> progress = gs.getProgressState().orElse(Map.of());
                final int completed = ((Number) progress.getOrDefault("completed", 0)).intValue();
                if (completed >= totalPartitions) {
                    return;
                }
                LOG.debug("Waiting for snapshot completion: {}/{}", completed, totalPartitions);
            }

            try {
                sourceCoordinator.saveProgressStateForPartition(
                        leaderPartition, DEFAULT_EXTEND_LEASE_DURATION);
            } catch (final Exception e) {
                LOG.warn("Failed to extend lease while waiting", e);
            }

            try {
                Thread.sleep(COMPLETION_CHECK_INTERVAL.toMillis());
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
