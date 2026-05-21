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

import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.iceberg.TableConfig;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.partition.ChangelogTaskPartition;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.partition.InitialLoadTaskPartition;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.partition.ShuffleReadPartition;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.partition.ShuffleWritePartition;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.state.ChangelogTaskProgressState;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.state.InitialLoadTaskProgressState;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.state.LeaderProgressState;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.state.ShuffleReadProgressState;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.state.ShuffleWriteProgressState;
import org.opensearch.dataprepper.plugins.source.iceberg.shuffle.ShuffleConfig;
import org.opensearch.dataprepper.plugins.source.iceberg.shuffle.ShuffleNodeClient;
import org.opensearch.dataprepper.plugins.source.iceberg.shuffle.ShufflePartitionCoalescer;
import org.opensearch.dataprepper.plugins.source.iceberg.shuffle.ShuffleStorage;
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class LeaderScheduler implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(LeaderScheduler.class);
    private static final Duration DEFAULT_EXTEND_LEASE_DURATION = Duration.ofMinutes(3);
    private static final Duration COMPLETION_CHECK_INTERVAL = Duration.ofSeconds(2);
    static final String SNAPSHOT_COMPLETION_PREFIX = "snapshot-completion-";
    public static final String SHUFFLE_FAILED_PREFIX = "shuffle-failed-";

    private final EnhancedSourceCoordinator sourceCoordinator;
    private final Map<String, TableConfig> tableConfigs;
    private final Duration pollingInterval;
    private final Map<String, Table> tables;
    private final ShuffleStorage shuffleStorage;
    private final ShuffleConfig shuffleConfig;
    private final Certificate certificate;
    private final TaskGrouper taskGrouper = new TaskGrouper();
    private LeaderPartition leaderPartition;

    public LeaderScheduler(final EnhancedSourceCoordinator sourceCoordinator,
                           final Map<String, TableConfig> tableConfigs,
                           final Duration pollingInterval,
                           final Map<String, Table> tables,
                           final ShuffleStorage shuffleStorage,
                           final ShuffleConfig shuffleConfig,
                           final Certificate certificate) {
        this.sourceCoordinator = sourceCoordinator;
        this.tableConfigs = tableConfigs;
        this.pollingInterval = pollingInterval;
        this.tables = tables;
        this.shuffleStorage = shuffleStorage;
        this.shuffleConfig = shuffleConfig;
        this.certificate = certificate;
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

            final String completionKey = SNAPSHOT_COMPLETION_PREFIX + "initial-" + snapshotId;
            sourceCoordinator.createPartition(new GlobalState(completionKey,
                    Map.of("total", 0, "completed", 0)));

            final TableScan scan = table.newScan().useSnapshot(snapshotId);
            int taskCount = 0;

            try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
                for (final FileScanTask task : tasks) {
                    final InitialLoadTaskProgressState taskState = new InitialLoadTaskProgressState();
                    taskState.setSnapshotId(snapshotId);
                    taskState.setTableName(tableName);
                    taskState.setDataFilePath(task.file().location());
                    taskState.setTotalRecords(task.file().recordCount());

                    final String partitionKey = tableName + "|initial|" + snapshotId + "|" + task.file().location();
                    sourceCoordinator.createPartition(new InitialLoadTaskPartition(partitionKey, taskState));
                    taskCount++;
                }
            } catch (final java.io.IOException e) {
                throw new RuntimeException("Failed to plan initial load for " + tableName, e);
            }

            LOG.info("Created {} initial load partition(s) for table {} snapshot {}",
                    taskCount, tableName, snapshotId);

            // Wait for all initial load partitions to complete
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

                // Scan once and decide path based on DELETED file presence
                final List<TaskGrouper.TaskInfo> taskInfos = scanChangelogTasks(
                        table, tableName, parentId, snapshot.snapshotId());
                final boolean hasDeleted = taskInfos.stream()
                        .anyMatch(t -> "DELETED".equals(t.taskType));

                if (hasDeleted) {
                    final TableConfig tableConfig = tableConfigs.get(tableName);
                    if (tableConfig.getIdentifierColumns().isEmpty()) {
                        throw new IllegalStateException(
                                "Snapshot " + snapshot.snapshotId() + " for table " + tableName
                                + " contains UPDATE/DELETE operations but identifier_columns is not configured. "
                                + "identifier_columns is required for correct CDC processing of UPDATE/DELETE.");
                    }
                    final List<ShuffleWriteProgressState> shuffleTasks =
                            taskGrouper.planShuffleWriteTasks(taskInfos, tableName, snapshot.snapshotId());
                    if (!processShuffleSnapshot(tableName, snapshot.snapshotId(), shuffleTasks)) {
                        LOG.warn("Shuffle failed for snapshot {}, will retry on next poll", snapshot.snapshotId());
                        break;
                    }
                } else {
                    processInsertOnlySnapshot(tableName, snapshot.snapshotId(), taskInfos);
                }

                progressState.setLastProcessedSnapshotId(snapshot.snapshotId());
                sourceCoordinator.saveProgressStateForPartition(
                        leaderPartition, DEFAULT_EXTEND_LEASE_DURATION);

                LOG.info("Snapshot {} completed for table {}", snapshot.snapshotId(), tableName);
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

    private List<TaskGrouper.TaskInfo> scanChangelogTasks(
            final Table table, final String tableName,
            final long fromSnapshotIdExclusive, final long toSnapshotId) {
        final IncrementalChangelogScan scan = table.newIncrementalChangelogScan()
                .fromSnapshotExclusive(fromSnapshotIdExclusive)
                .toSnapshot(toSnapshotId);

        final List<TaskGrouper.TaskInfo> taskInfos = new ArrayList<>();
        try (CloseableIterable<ChangelogScanTask> tasks = scan.planFiles()) {
            for (final ChangelogScanTask task : tasks) {
                taskInfos.add(TaskGrouper.TaskInfo.from(task));
            }
        } catch (final IOException e) {
            throw new RuntimeException("Failed to plan changelog scan for " + tableName, e);
        }
        return taskInfos;
    }

    private void processInsertOnlySnapshot(final String tableName, final long snapshotId,
                                            final List<TaskGrouper.TaskInfo> taskInfos) {
        final List<ChangelogTaskProgressState> taskGroups =
                taskGrouper.planInsertOnlyTasks(taskInfos, tableName, snapshotId);

        if (taskGroups.isEmpty()) {
            return;
        }

        final String completionKey = SNAPSHOT_COMPLETION_PREFIX + snapshotId;
        sourceCoordinator.createPartition(new GlobalState(completionKey,
                Map.of("total", taskGroups.size(), "completed", 0)));

        for (final ChangelogTaskProgressState taskState : taskGroups) {
            final String filesHash = deterministicHash(taskState.getDataFilePaths());
            final String partitionKey = tableName + "|" + snapshotId + "|" + filesHash;
            sourceCoordinator.createPartition(new ChangelogTaskPartition(partitionKey, taskState));
        }

        LOG.info("Created {} INSERT-only partition(s) for snapshot {}", taskGroups.size(), snapshotId);
        waitForSnapshotComplete(completionKey, taskGroups.size());
    }

    private boolean processShuffleSnapshot(final String tableName, final long snapshotId,
                                         final List<ShuffleWriteProgressState> shuffleTasks) {
        final String snapshotIdStr = String.valueOf(snapshotId);

        // Phase 1: Create SHUFFLE_WRITE tasks
        // Create location tracker GlobalState (workers will write their address here on completion)
        final String locationKey = "shuffle-locations-" + snapshotIdStr;
        sourceCoordinator.createPartition(new GlobalState(locationKey, new java.util.HashMap<>()));

        // Create completion key before partitions to avoid race condition where workers
        // complete and try to increment before the key exists
        final String writeCompletionKey = SNAPSHOT_COMPLETION_PREFIX + "sw-" + snapshotId;
        sourceCoordinator.createPartition(new GlobalState(writeCompletionKey,
                Map.of("total", shuffleTasks.size(), "completed", 0)));

        for (final ShuffleWriteProgressState taskState : shuffleTasks) {
            final String shuffleTaskId = deterministicHash(List.of(taskState.getDataFilePath()));
            taskState.setShuffleTaskId(shuffleTaskId);

            final String partitionKey = tableName + "|sw|" + snapshotId + "|" + shuffleTaskId;
            sourceCoordinator.createPartition(new ShuffleWritePartition(partitionKey, taskState));
        }

        LOG.info("Created {} SHUFFLE_WRITE task(s) for snapshot {}", shuffleTasks.size(), snapshotId);
        waitForShuffleComplete(writeCompletionKey, shuffleTasks.size(), snapshotIdStr);

        // Check if shuffle failed
        if (isShuffleFailed(snapshotIdStr)) {
            LOG.warn("Shuffle failed for snapshot {}, skipping", snapshotId);
            cleanupAllNodes(snapshotIdStr, locationKey);
            return false;
        }

        // Barrier: collect index data from all nodes and coalesce
        // Read shuffle write locations from GlobalState first (need node addresses to fetch remote indexes)
        final Optional<EnhancedSourcePartition> locationPartition = sourceCoordinator.getPartition(locationKey);
        final Map<String, Object> locationMap = locationPartition.map(enhancedSourcePartition -> ((GlobalState) enhancedSourcePartition).getProgressState().orElse(Map.of())).orElseGet(Map::of);

        final List<String> completedTaskIds = new ArrayList<>();
        final List<String> completedNodeAddresses = new ArrayList<>();
        for (final Map.Entry<String, Object> entry : locationMap.entrySet()) {
            completedTaskIds.add(entry.getKey());
            completedNodeAddresses.add(String.valueOf(entry.getValue()));
            LOG.debug("Shuffle write location: taskId={} nodeAddress={} valueType={}",
                    entry.getKey(), entry.getValue(),
                    entry.getValue() != null ? entry.getValue().getClass().getSimpleName() : "null");
        }
        LOG.info("Collected {} shuffle write locations for snapshot {}", completedTaskIds.size(), snapshotId);

        final int numPartitions = shuffleConfig.getPartitions();
        final ShuffleNodeClient client = new ShuffleNodeClient(shuffleConfig, certificate);
        final long[] partitionSizes = client.collectPartitionSizes(
                shuffleStorage, snapshotIdStr, completedTaskIds, completedNodeAddresses, numPartitions);

        final ShufflePartitionCoalescer coalescer =
                new ShufflePartitionCoalescer(shuffleConfig.getTargetPartitionSizeBytes());
        final List<ShufflePartitionCoalescer.PartitionRange> ranges = coalescer.coalesce(partitionSizes);

        if (ranges.isEmpty()) {
            LOG.info("No data after shuffle for snapshot {}", snapshotId);
            cleanupAllNodes(snapshotIdStr, locationKey);
            return true;
        }

        // Phase 2: Create SHUFFLE_READ tasks
        final String readCompletionKey = SNAPSHOT_COMPLETION_PREFIX + "sr-" + snapshotId;
        sourceCoordinator.createPartition(new GlobalState(readCompletionKey,
                Map.of("total", ranges.size(), "completed", 0)));

        for (final ShufflePartitionCoalescer.PartitionRange range : ranges) {
            final ShuffleReadProgressState readState = new ShuffleReadProgressState();
            readState.setSnapshotId(snapshotId);
            readState.setTableName(tableName);
            readState.setPartitionRangeStart(range.getStartPartition());
            readState.setPartitionRangeEnd(range.getEndPartitionInclusive());
            readState.setShuffleWriteTaskIds(completedTaskIds);
            readState.setNodeAddresses(completedNodeAddresses);

            final String partitionKey = tableName + "|sr|" + snapshotId + "|" + range.getStartPartition() + "-" + range.getEndPartitionInclusive();
            sourceCoordinator.createPartition(new ShuffleReadPartition(partitionKey, readState));
        }

        LOG.info("Created {} SHUFFLE_READ task(s) for snapshot {} (coalesced from {} partitions)",
                ranges.size(), snapshotId, numPartitions);
        waitForShuffleComplete(readCompletionKey, ranges.size(), snapshotIdStr);

        if (isShuffleFailed(snapshotIdStr)) {
            LOG.warn("Shuffle read failed for snapshot {}, skipping", snapshotId);
            cleanupAllNodes(snapshotIdStr, locationKey);
            return false;
        }

        cleanupAllNodes(snapshotIdStr, locationKey);
        return true;
    }

    private void waitForShuffleComplete(final String completionKey, final int totalPartitions,
                                         final String snapshotIdStr) {
        while (!Thread.currentThread().isInterrupted()) {
            if (isShuffleFailed(snapshotIdStr)) {
                return;
            }

            final Optional<EnhancedSourcePartition> state =
                    sourceCoordinator.getPartition(completionKey);

            if (state.isPresent()) {
                final GlobalState gs = (GlobalState) state.get();
                final Map<String, Object> progress = gs.getProgressState().orElse(Map.of());
                final int completed = ((Number) progress.getOrDefault("completed", 0)).intValue();
                if (completed >= totalPartitions) {
                    return;
                }
            }

            try {
                sourceCoordinator.saveProgressStateForPartition(
                        leaderPartition, DEFAULT_EXTEND_LEASE_DURATION);
            } catch (final Exception e) {
                LOG.warn("Failed to extend lease while waiting for shuffle", e);
            }

            try {
                Thread.sleep(COMPLETION_CHECK_INTERVAL.toMillis());
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private boolean isShuffleFailed(final String snapshotIdStr) {
        final Optional<EnhancedSourcePartition> failedState =
                sourceCoordinator.getPartition(SHUFFLE_FAILED_PREFIX + snapshotIdStr);
        if (failedState.isPresent()) {
            final GlobalState gs = (GlobalState) failedState.get();
            final Map<String, Object> progress = gs.getProgressState().orElse(Map.of());
            return Boolean.TRUE.equals(progress.get("failed"));
        }
        return false;
    }

    private void cleanupAllNodes(final String snapshotId, final String locationKey) {
        shuffleStorage.cleanup(snapshotId);
        final Optional<EnhancedSourcePartition> locationPartition = sourceCoordinator.getPartition(locationKey);
        locationPartition.ifPresent(p -> {
            final Map<String, Object> locations = ((GlobalState) p).getProgressState().orElse(Map.of());
            final ShuffleNodeClient client = new ShuffleNodeClient(shuffleConfig, certificate);
            locations.values().stream()
                    .map(String::valueOf)
                    .distinct()
                    .filter(addr -> !ShuffleNodeClient.isLocalAddress(addr))
                    .forEach(addr -> client.requestCleanup(addr, snapshotId));
        });
    }

    private static String deterministicHash(final List<String> values) {
        final List<String> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        try {
            final MessageDigest md = MessageDigest.getInstance("SHA-256");
            for (final String v : sorted) {
                md.update(v.getBytes(StandardCharsets.UTF_8));
            }
            final byte[] digest = md.digest();
            final StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 8; i++) {
                sb.append(String.format("%02x", digest[i]));
            }
            return sb.toString();
        } catch (final NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
