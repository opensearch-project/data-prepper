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

/**
 * Abstraction for shuffle intermediate storage.
 * Initial implementation uses local disk. Future implementations may use other storages like S3.
 */
public interface ShuffleStorage {

    ShuffleWriter createWriter(String snapshotId, String taskId, int numPartitions);

    ShuffleReader createReader(String snapshotId, String taskId);

    /**
     * Returns the total byte size of each hash partition for a given snapshot,
     * summed across all shuffle write tasks.
     *
     * @param snapshotId the snapshot to query
     * @param numPartitions the number of hash partitions (e.g. 64)
     * @return an array of length {@code numPartitions} where element at index {@code p}
     *         is the total bytes written to hash partition {@code p} across all tasks
     */
    long[] getPartitionSizes(String snapshotId, int numPartitions);

    /**
     * Returns all task IDs that have shuffle files for a given snapshot.
     */
    java.util.List<String> getTaskIds(String snapshotId);

    void cleanup(String snapshotId);

    void cleanupAll();
}
