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
import org.apache.iceberg.DeletedDataFileScanTask;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.state.ChangelogTaskProgressState;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.state.ShuffleWriteProgressState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts scan results into task states for distributed processing.
 * <p>
 * For INSERT-only snapshots, each data file becomes an independent ChangelogTask.
 * For snapshots containing DELETED files, each data file becomes a SHUFFLE_WRITE task.
 */
public class TaskGrouper {

    private static final Logger LOG = LoggerFactory.getLogger(TaskGrouper.class);

    /**
     * Converts scan results into independent ChangelogTask states for INSERT-only snapshots.
     */
    public List<ChangelogTaskProgressState> planInsertOnlyTasks(
            final List<TaskInfo> taskInfos,
            final String tableName,
            final long snapshotId) {

        final List<ChangelogTaskProgressState> result = new ArrayList<>();
        for (final TaskInfo info : taskInfos) {
            final ChangelogTaskProgressState state = new ChangelogTaskProgressState();
            state.setSnapshotId(snapshotId);
            state.setTableName(tableName);
            state.setDataFilePaths(List.of(info.filePath));
            state.setTaskTypes(List.of(info.taskType));
            state.setTotalRecords(info.recordCount);
            result.add(state);
        }

        LOG.info("Planned {} CHANGELOG task(s) for table {} (snapshot {})",
                result.size(), tableName, snapshotId);
        return result;
    }

    /**
     * Converts scan results into SHUFFLE_WRITE task states for snapshots containing DELETED files.
     */
    public List<ShuffleWriteProgressState> planShuffleWriteTasks(
            final List<TaskInfo> taskInfos,
            final String tableName,
            final long snapshotId) {

        final List<ShuffleWriteProgressState> tasks = new ArrayList<>();
        for (final TaskInfo info : taskInfos) {
            final ShuffleWriteProgressState state = new ShuffleWriteProgressState();
            state.setSnapshotId(snapshotId);
            state.setTableName(tableName);
            state.setDataFilePath(info.filePath);
            state.setTaskType(info.taskType);
            state.setChangeOrdinal(info.changeOrdinal);
            tasks.add(state);
        }

        LOG.info("Planned {} SHUFFLE_WRITE task(s) for table {} (snapshot {})",
                tasks.size(), tableName, snapshotId);
        return tasks;
    }

    public static class TaskInfo {
        final String filePath;
        final String taskType;
        final long recordCount;
        final int changeOrdinal;

        public TaskInfo(final String filePath, final String taskType,
                 final long recordCount, final int changeOrdinal) {
            this.filePath = filePath;
            this.taskType = taskType;
            this.recordCount = recordCount;
            this.changeOrdinal = changeOrdinal;
        }

        public static TaskInfo from(final ChangelogScanTask task) {
            if (task instanceof AddedRowsScanTask) {
                final AddedRowsScanTask t = (AddedRowsScanTask) task;
                return new TaskInfo(t.file().location(), "ADDED",
                        t.file().recordCount(), task.changeOrdinal());
            } else if (task instanceof DeletedDataFileScanTask) {
                final DeletedDataFileScanTask t = (DeletedDataFileScanTask) task;
                return new TaskInfo(t.file().location(), "DELETED",
                        t.file().recordCount(), task.changeOrdinal());
            }
            throw new IllegalArgumentException("Unsupported ChangelogScanTask type: " + task.getClass().getName());
        }
    }
}
