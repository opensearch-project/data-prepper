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

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

class TaskGrouperTest {

    private final TaskGrouper taskGrouper = new TaskGrouper();

    @Test
    void pairByBounds_insertOnly_eachFileIsIndependentGroup() {
        final List<TaskGrouper.TaskInfo> tasks = List.of(
                new TaskGrouper.TaskInfo("file1", "ADDED", 100, "bounds1"),
                new TaskGrouper.TaskInfo("file2", "ADDED", 200, "bounds2")
        );
        final List<List<TaskGrouper.TaskInfo>> groups = taskGrouper.pairByBounds(tasks);
        assertThat(groups, hasSize(2));
    }

    @Test
    void pairByBounds_deleteOnly_eachFileIsIndependentGroup() {
        final List<TaskGrouper.TaskInfo> tasks = List.of(
                new TaskGrouper.TaskInfo("file1", "DELETED", 100, "bounds1")
        );
        final List<List<TaskGrouper.TaskInfo>> groups = taskGrouper.pairByBounds(tasks);
        assertThat(groups, hasSize(1));
    }

    @Test
    void pairByBounds_matchingBounds_pairedTogether() {
        final List<TaskGrouper.TaskInfo> tasks = List.of(
                new TaskGrouper.TaskInfo("old_file", "DELETED", 100, "{1=abc}|{1=xyz}"),
                new TaskGrouper.TaskInfo("new_file", "ADDED", 100, "{1=abc}|{1=xyz}")
        );
        final List<List<TaskGrouper.TaskInfo>> groups = taskGrouper.pairByBounds(tasks);
        assertThat(groups, hasSize(1));
        assertThat(groups.get(0), hasSize(2));
    }

    @Test
    void pairByBounds_differentBounds_fallbackGroup() {
        final List<TaskGrouper.TaskInfo> tasks = List.of(
                new TaskGrouper.TaskInfo("old_file", "DELETED", 100, "{1=abc}|{1=xyz}"),
                new TaskGrouper.TaskInfo("new_file", "ADDED", 100, "{1=abc}|{1=zzz}")
        );
        final List<List<TaskGrouper.TaskInfo>> groups = taskGrouper.pairByBounds(tasks);
        // Bounds don't match -> fallback group with both
        assertThat(groups, hasSize(1));
        assertThat(groups.get(0), hasSize(2));
    }

    @Test
    void pairByBounds_multiplePairs_eachPairedSeparately() {
        final List<TaskGrouper.TaskInfo> tasks = List.of(
                new TaskGrouper.TaskInfo("old_us", "DELETED", 100, "bounds_us"),
                new TaskGrouper.TaskInfo("old_eu", "DELETED", 100, "bounds_eu"),
                new TaskGrouper.TaskInfo("new_us", "ADDED", 100, "bounds_us"),
                new TaskGrouper.TaskInfo("new_eu", "ADDED", 100, "bounds_eu")
        );
        final List<List<TaskGrouper.TaskInfo>> groups = taskGrouper.pairByBounds(tasks);
        assertThat(groups, hasSize(2));
        assertThat(groups.get(0), hasSize(2));
        assertThat(groups.get(1), hasSize(2));
    }

    @Test
    void pairByBounds_ambiguousBounds_fallbackGroup() {
        // Two DELETED and two ADDED with same bounds -> can't pair uniquely
        final List<TaskGrouper.TaskInfo> tasks = List.of(
                new TaskGrouper.TaskInfo("old1", "DELETED", 100, "same_bounds"),
                new TaskGrouper.TaskInfo("old2", "DELETED", 100, "same_bounds"),
                new TaskGrouper.TaskInfo("new1", "ADDED", 100, "same_bounds"),
                new TaskGrouper.TaskInfo("new2", "ADDED", 100, "same_bounds")
        );
        final List<List<TaskGrouper.TaskInfo>> groups = taskGrouper.pairByBounds(tasks);
        // Ambiguous -> all in one fallback group
        assertThat(groups, hasSize(1));
        assertThat(groups.get(0), hasSize(4));
    }

    @Test
    void pairByBounds_nullBounds_fallbackGroup() {
        final List<TaskGrouper.TaskInfo> tasks = List.of(
                new TaskGrouper.TaskInfo("old_file", "DELETED", 100, null),
                new TaskGrouper.TaskInfo("new_file", "ADDED", 100, null)
        );
        final List<List<TaskGrouper.TaskInfo>> groups = taskGrouper.pairByBounds(tasks);
        // Null bounds -> can't pair -> fallback
        assertThat(groups, hasSize(1));
        assertThat(groups.get(0), hasSize(2));
    }
}
