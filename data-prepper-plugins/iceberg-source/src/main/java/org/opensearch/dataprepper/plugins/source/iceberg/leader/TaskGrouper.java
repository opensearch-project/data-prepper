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

import org.apache.iceberg.AddedRowsScanTask;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DeletedDataFileScanTask;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.state.ChangelogTaskProgressState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Groups ChangelogScanTasks for distributed processing.
 *
 * Grouping stages:
 * 1. Iceberg partition isolation: tasks grouped by partition value
 * 2. Bounds-based pairing: within a partition, DELETED-ADDED file pairs with matching
 *    bounds are split into individual groups for maximum distribution
 */
public class TaskGrouper {

    private static final Logger LOG = LoggerFactory.getLogger(TaskGrouper.class);
    private static final String UNPARTITIONED_KEY = "__unpartitioned__";

    public List<ChangelogTaskProgressState> planAndGroup(
            final Table table,
            final String tableName,
            final long fromSnapshotIdExclusive,
            final long toSnapshotId) {

        final IncrementalChangelogScan scan = table.newIncrementalChangelogScan()
                .fromSnapshotExclusive(fromSnapshotIdExclusive)
                .toSnapshot(toSnapshotId)
                .includeColumnStats();

        // Stage 1: Group by Iceberg partition
        final Map<String, List<TaskInfo>> tasksByPartition = new HashMap<>();

        try (CloseableIterable<ChangelogScanTask> tasks = scan.planFiles()) {
            for (final ChangelogScanTask task : tasks) {
                final String partitionKey = extractPartitionKey(task);
                tasksByPartition
                        .computeIfAbsent(partitionKey, k -> new ArrayList<>())
                        .add(TaskInfo.from(task));
            }
        } catch (final IOException e) {
            throw new RuntimeException("Failed to plan changelog scan for " + tableName, e);
        }

        // Stage 2: Within each partition, attempt bounds-based pairing
        final List<ChangelogTaskProgressState> result = new ArrayList<>();

        for (final Map.Entry<String, List<TaskInfo>> entry : tasksByPartition.entrySet()) {
            final List<TaskInfo> partitionTasks = entry.getValue();
            final List<List<TaskInfo>> groups = pairByBounds(partitionTasks);

            for (final List<TaskInfo> group : groups) {
                final ChangelogTaskProgressState state = new ChangelogTaskProgressState();
                state.setSnapshotId(toSnapshotId);
                state.setTableName(tableName);
                state.setDataFilePaths(group.stream().map(t -> t.filePath).collect(java.util.stream.Collectors.toList()));
                state.setTaskTypes(group.stream().map(t -> t.taskType).collect(java.util.stream.Collectors.toList()));
                state.setTotalRecords(group.stream().mapToLong(t -> t.recordCount).sum());
                result.add(state);
            }
        }

        LOG.info("Planned {} task group(s) for table {} (snapshot {} -> {})",
                result.size(), tableName, fromSnapshotIdExclusive, toSnapshotId);
        return result;
    }

    /**
     * Pairs DELETED and ADDED tasks by matching lower/upper bounds.
     * Returns a list of groups, where each group is a list of tasks to process together.
     */
    List<List<TaskInfo>> pairByBounds(final List<TaskInfo> tasks) {
        final List<TaskInfo> deleted = new ArrayList<>();
        final List<TaskInfo> added = new ArrayList<>();

        for (final TaskInfo task : tasks) {
            if ("DELETED".equals(task.taskType)) {
                deleted.add(task);
            } else {
                added.add(task);
            }
        }

        // If no deleted files, each added file is an independent group (INSERT only)
        if (deleted.isEmpty()) {
            final List<List<TaskInfo>> result = new ArrayList<>();
            for (final TaskInfo task : added) {
                result.add(List.of(task));
            }
            return result;
        }

        // If no added files, each deleted file is an independent group (full-file delete)
        if (added.isEmpty()) {
            final List<List<TaskInfo>> result = new ArrayList<>();
            for (final TaskInfo task : deleted) {
                result.add(List.of(task));
            }
            return result;
        }

        // Try to pair DELETED-ADDED by bounds
        final List<List<TaskInfo>> paired = new ArrayList<>();
        final List<TaskInfo> unpairedDeleted = new ArrayList<>(deleted);
        final List<TaskInfo> unpairedAdded = new ArrayList<>(added);

        final Iterator<TaskInfo> delIter = unpairedDeleted.iterator();
        while (delIter.hasNext()) {
            final TaskInfo del = delIter.next();
            TaskInfo matchedAdd = null;
            int matchCount = 0;

            for (final TaskInfo add : unpairedAdded) {
                if (boundsMatch(del, add)) {
                    matchedAdd = add;
                    matchCount++;
                }
            }

            // Only pair if exactly one match (unambiguous)
            if (matchCount == 1) {
                paired.add(List.of(del, matchedAdd));
                delIter.remove();
                unpairedAdded.remove(matchedAdd);
            }
        }

        // Unpaired DELETED-only or ADDED-only -> individual groups
        for (final TaskInfo task : unpairedDeleted) {
            if (unpairedAdded.isEmpty()) {
                paired.add(List.of(task));
            }
        }
        for (final TaskInfo task : unpairedAdded) {
            if (unpairedDeleted.isEmpty()) {
                paired.add(List.of(task));
            }
        }

        // If both unpaired DELETED and ADDED remain -> fallback group
        if (!unpairedDeleted.isEmpty() && !unpairedAdded.isEmpty()) {
            final List<TaskInfo> fallback = new ArrayList<>();
            fallback.addAll(unpairedDeleted);
            fallback.addAll(unpairedAdded);
            paired.add(fallback);
        }

        return paired;
    }

    private boolean boundsMatch(final TaskInfo a, final TaskInfo b) {
        if (a.boundsKey == null || b.boundsKey == null) {
            return false;
        }
        return a.boundsKey.equals(b.boundsKey);
    }

    private String extractPartitionKey(final ChangelogScanTask task) {
        if (task instanceof ContentScanTask) {
            final StructLike partition = ((ContentScanTask<?>) task).file().partition();
            if (partition.size() == 0) {
                return UNPARTITIONED_KEY;
            }
            final StringBuilder sb = new StringBuilder();
            for (int i = 0; i < partition.size(); i++) {
                if (i > 0) {
                    sb.append("|");
                }
                sb.append(partition.get(i, Object.class));
            }
            return sb.toString();
        }
        return UNPARTITIONED_KEY;
    }

    static class TaskInfo {
        final String filePath;
        final String taskType;
        final long recordCount;
        final String boundsKey; // serialized lower+upper bounds for pairing

        TaskInfo(final String filePath, final String taskType,
                 final long recordCount, final String boundsKey) {
            this.filePath = filePath;
            this.taskType = taskType;
            this.recordCount = recordCount;
            this.boundsKey = boundsKey;
        }

        static TaskInfo from(final ChangelogScanTask task) {
            if (task instanceof AddedRowsScanTask) {
                final AddedRowsScanTask t = (AddedRowsScanTask) task;
                return new TaskInfo(
                        t.file().path().toString(), "ADDED",
                        t.file().recordCount(), extractBoundsKey(t));
            } else if (task instanceof DeletedDataFileScanTask) {
                final DeletedDataFileScanTask t = (DeletedDataFileScanTask) task;
                return new TaskInfo(
                        t.file().path().toString(), "DELETED",
                        t.file().recordCount(), extractBoundsKey(t));
            }
            return new TaskInfo("unknown", "UNKNOWN", 0, null);
        }

        private static String extractBoundsKey(final ContentScanTask<?> task) {
            final var lower = task.file().lowerBounds();
            final var upper = task.file().upperBounds();
            if (lower == null || upper == null || lower.isEmpty() || upper.isEmpty()) {
                return null;
            }
            return lower.toString() + "|" + upper.toString();
        }
    }
}
