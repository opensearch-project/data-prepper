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

import java.util.ArrayList;
import java.util.List;

/**
 * Coalesces shuffle partitions based on data size, similar to Spark's AQE
 * (Adaptive Query Execution) coalescing of post-shuffle partitions.
 * <p>
 * Empty partitions are skipped. Adjacent small partitions are merged until
 * the target size is reached.
 *
 * @see <a href="https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/ShufflePartitionsUtil.scala">
 *     Spark ShufflePartitionsUtil</a>
 */
public class ShufflePartitionCoalescer {

    private final long targetSizeBytes;

    public ShufflePartitionCoalescer(final long targetSizeBytes) {
        this.targetSizeBytes = targetSizeBytes;
    }

    /**
     * @param partitionSizes size in bytes for each partition (index = partition number)
     * @return list of partition ranges, each representing one SHUFFLE_READ task
     */
    public List<PartitionRange> coalesce(final long[] partitionSizes) {
        final List<PartitionRange> result = new ArrayList<>();
        int rangeStart = -1;
        long currentSize = 0;

        for (int i = 0; i < partitionSizes.length; i++) {
            if (partitionSizes[i] == 0) {
                continue;
            }
            if (rangeStart == -1) {
                rangeStart = i;
                currentSize = partitionSizes[i];
            } else if (currentSize + partitionSizes[i] > targetSizeBytes) {
                result.add(new PartitionRange(rangeStart, i - 1));
                rangeStart = i;
                currentSize = partitionSizes[i];
            } else {
                currentSize += partitionSizes[i];
            }
        }

        if (rangeStart != -1) {
            result.add(new PartitionRange(rangeStart, partitionSizes.length - 1));
        }

        return result;
    }

    public static class PartitionRange {
        private final int startPartition;
        private final int endPartitionInclusive;

        public PartitionRange(final int startPartition, final int endPartitionInclusive) {
            this.startPartition = startPartition;
            this.endPartitionInclusive = endPartitionInclusive;
        }

        public int getStartPartition() { return startPartition; }
        public int getEndPartitionInclusive() { return endPartitionInclusive; }
    }
}
