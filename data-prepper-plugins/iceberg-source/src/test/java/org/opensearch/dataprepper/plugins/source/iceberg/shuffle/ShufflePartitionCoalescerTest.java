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

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

class ShufflePartitionCoalescerTest {

    @Test
    void allEmpty_returnsNoRanges() {
        final var coalescer = new ShufflePartitionCoalescer(64);
        final List<ShufflePartitionCoalescer.PartitionRange> result =
                coalescer.coalesce(new long[]{0, 0, 0, 0});
        assertThat(result, is(empty()));
    }

    @Test
    void singleNonEmpty_returnsSingleRange() {
        final var coalescer = new ShufflePartitionCoalescer(100);
        final List<ShufflePartitionCoalescer.PartitionRange> result =
                coalescer.coalesce(new long[]{0, 0, 50, 0});
        assertThat(result, hasSize(1));
        assertThat(result.get(0).getStartPartition(), is(2));
        assertThat(result.get(0).getEndPartitionInclusive(), is(3));
    }

    @Test
    void smallPartitions_coalescedTogether() {
        final var coalescer = new ShufflePartitionCoalescer(100);
        // 10 + 20 + 30 = 60 < 100, all fit in one range
        final List<ShufflePartitionCoalescer.PartitionRange> result =
                coalescer.coalesce(new long[]{10, 20, 30, 0});
        assertThat(result, hasSize(1));
        assertThat(result.get(0).getStartPartition(), is(0));
    }

    @Test
    void largePartitions_splitIntoSeparateRanges() {
        final var coalescer = new ShufflePartitionCoalescer(100);
        // 80 + 90 > 100, must split
        final List<ShufflePartitionCoalescer.PartitionRange> result =
                coalescer.coalesce(new long[]{80, 90, 10});
        assertThat(result, hasSize(2));
        assertThat(result.get(0).getStartPartition(), is(0));
        assertThat(result.get(0).getEndPartitionInclusive(), is(0));
        assertThat(result.get(1).getStartPartition(), is(1));
        assertThat(result.get(1).getEndPartitionInclusive(), is(2));
    }

    @Test
    void emptyPartitions_skippedInCoalescing() {
        final var coalescer = new ShufflePartitionCoalescer(100);
        // partitions: 10, 0, 0, 20, 0, 30 -> coalesced as 10+20+30=60
        final List<ShufflePartitionCoalescer.PartitionRange> result =
                coalescer.coalesce(new long[]{10, 0, 0, 20, 0, 30});
        assertThat(result, hasSize(1));
        assertThat(result.get(0).getStartPartition(), is(0));
        assertThat(result.get(0).getEndPartitionInclusive(), is(5));
    }

    @Test
    void eachPartitionExceedsTarget_oneRangePerPartition() {
        final var coalescer = new ShufflePartitionCoalescer(50);
        final List<ShufflePartitionCoalescer.PartitionRange> result =
                coalescer.coalesce(new long[]{60, 70, 80});
        assertThat(result, hasSize(3));
    }
}
