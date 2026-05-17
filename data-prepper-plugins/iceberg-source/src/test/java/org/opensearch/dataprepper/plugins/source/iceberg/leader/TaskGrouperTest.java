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

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.state.ChangelogTaskProgressState;
import org.opensearch.dataprepper.plugins.source.iceberg.coordination.state.ShuffleWriteProgressState;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

class TaskGrouperTest {

    private final TaskGrouper taskGrouper = new TaskGrouper();

    @Test
    void planInsertOnlyTasks_creates_one_task_per_file() {
        final List<TaskGrouper.TaskInfo> taskInfos = List.of(
                new TaskGrouper.TaskInfo("file1.parquet", "ADDED", 100, 0),
                new TaskGrouper.TaskInfo("file2.parquet", "ADDED", 200, 0)
        );

        final List<ChangelogTaskProgressState> result =
                taskGrouper.planInsertOnlyTasks(taskInfos, "db.table", 12345L);

        assertThat(result, hasSize(2));
        assertThat(result.get(0).getDataFilePaths(), equalTo(List.of("file1.parquet")));
        assertThat(result.get(0).getTotalRecords(), equalTo(100L));
        assertThat(result.get(1).getDataFilePaths(), equalTo(List.of("file2.parquet")));
        assertThat(result.get(1).getTotalRecords(), equalTo(200L));
    }

    @Test
    void planShuffleWriteTasks_creates_one_task_per_file_with_change_ordinal() {
        final List<TaskGrouper.TaskInfo> taskInfos = List.of(
                new TaskGrouper.TaskInfo("deleted.parquet", "DELETED", 100, 0),
                new TaskGrouper.TaskInfo("added.parquet", "ADDED", 100, 0)
        );

        final List<ShuffleWriteProgressState> result =
                taskGrouper.planShuffleWriteTasks(taskInfos, "db.table", 12345L);

        assertThat(result, hasSize(2));
        assertThat(result.get(0).getDataFilePath(), equalTo("deleted.parquet"));
        assertThat(result.get(0).getTaskType(), equalTo("DELETED"));
        assertThat(result.get(0).getChangeOrdinal(), equalTo(0));
        assertThat(result.get(1).getDataFilePath(), equalTo("added.parquet"));
        assertThat(result.get(1).getTaskType(), equalTo("ADDED"));
    }

    @Test
    void hasDeleted_is_true_when_deleted_task_exists() {
        final List<TaskGrouper.TaskInfo> taskInfos = List.of(
                new TaskGrouper.TaskInfo("deleted.parquet", "DELETED", 100, 0),
                new TaskGrouper.TaskInfo("added.parquet", "ADDED", 100, 0)
        );

        final boolean hasDeleted = taskInfos.stream()
                .anyMatch(t -> "DELETED".equals(t.taskType));

        assertThat(hasDeleted, equalTo(true));
    }

    @Test
    void hasDeleted_is_false_for_insert_only() {
        final List<TaskGrouper.TaskInfo> taskInfos = List.of(
                new TaskGrouper.TaskInfo("file1.parquet", "ADDED", 100, 0),
                new TaskGrouper.TaskInfo("file2.parquet", "ADDED", 200, 0)
        );

        final boolean hasDeleted = taskInfos.stream()
                .anyMatch(t -> "DELETED".equals(t.taskType));

        assertThat(hasDeleted, equalTo(false));
    }
}
