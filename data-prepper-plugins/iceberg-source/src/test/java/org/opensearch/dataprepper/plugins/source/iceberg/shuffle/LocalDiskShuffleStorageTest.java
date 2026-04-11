/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.iceberg.shuffle;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

class LocalDiskShuffleStorageTest {

    @TempDir
    Path tempDir;

    private LocalDiskShuffleStorage storage;

    @BeforeEach
    void setUp() {
        storage = new LocalDiskShuffleStorage(tempDir);
    }

    @Test
    void getTaskIds_returnsAllTasks() throws Exception {
        writeTask("snap1", "taskA", 4);
        writeTask("snap1", "taskB", 4);

        final List<String> taskIds = storage.getTaskIds("snap1");
        assertThat(taskIds, hasSize(2));
        assertThat(taskIds, containsInAnyOrder("taskA", "taskB"));
    }

    @Test
    void getTaskIds_nonExistentSnapshot_returnsEmpty() {
        assertThat(storage.getTaskIds("nonexistent"), is(empty()));
    }

    @Test
    void getPartitionSizes_aggregatesAcrossTasks() throws Exception {
        // taskA: partition 0 has data, partition 1 empty
        try (ShuffleWriter writer = storage.createWriter("snap1", "taskA", 2)) {
            writer.addRecord(0, ShuffleRecord.OP_INSERT, 0, new byte[]{1, 2, 3});
            writer.finish();
        }
        // taskB: partition 0 has data, partition 1 has data
        try (ShuffleWriter writer = storage.createWriter("snap1", "taskB", 2)) {
            writer.addRecord(0, ShuffleRecord.OP_INSERT, 0, new byte[]{4, 5});
            writer.addRecord(1, ShuffleRecord.OP_DELETE, 0, new byte[]{6, 7, 8, 9});
            writer.finish();
        }

        final long[] sizes = storage.getPartitionSizes("snap1", 2);
        assertThat(sizes.length, is(2));
        // Partition 0: data from both tasks
        assertThat(sizes[0] > 0, is(true));
        // Partition 1: data from taskB only
        assertThat(sizes[1] > 0, is(true));
        // Partition 0 should be larger (more records)
        assertThat(sizes[0] > sizes[1], is(true));
    }

    @Test
    void cleanup_removesSnapshotDirectory() throws Exception {
        writeTask("snap1", "task1", 2);
        assertThat(Files.exists(tempDir.resolve("snap1")), is(true));

        storage.cleanup("snap1");
        assertThat(Files.exists(tempDir.resolve("snap1")), is(false));
    }

    @Test
    void cleanupAll_removesContentsButKeepsBaseDirectory() throws Exception {
        writeTask("snap1", "task1", 2);
        writeTask("snap2", "task1", 2);

        storage.cleanupAll();
        assertThat(Files.exists(tempDir), is(true));
        assertThat(storage.getTaskIds("snap1"), hasSize(0));
        assertThat(storage.getTaskIds("snap2"), hasSize(0));
    }

    @Test
    void cleanup_nonExistentSnapshot_doesNotThrow() {
        storage.cleanup("nonexistent");
    }

    private void writeTask(final String snapshotId, final String taskId, final int numPartitions) throws Exception {
        try (ShuffleWriter writer = storage.createWriter(snapshotId, taskId, numPartitions)) {
            writer.addRecord(0, ShuffleRecord.OP_INSERT, 0, new byte[]{1});
            writer.finish();
        }
    }
}
