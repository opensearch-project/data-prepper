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

import java.nio.file.Path;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

class LocalDiskShuffleWriterReaderTest {

    @TempDir
    Path tempDir;

    private LocalDiskShuffleStorage storage;

    @BeforeEach
    void setUp() {
        storage = new LocalDiskShuffleStorage(tempDir);
    }

    @Test
    void roundTrip_singlePartition() throws Exception {
        final byte[] data1 = {1, 2, 3};
        final byte[] data2 = {4, 5, 6, 7};

        try (ShuffleWriter writer = storage.createWriter("snap1", "task1", 1)) {
            writer.addRecord(0, ShuffleRecord.OP_INSERT, 0, data1);
            writer.addRecord(0, ShuffleRecord.OP_DELETE, 0, data2);
            writer.finish();
        }

        try (ShuffleReader reader = storage.createReader("snap1", "task1")) {
            final List<ShuffleRecord> records = reader.readPartitions(0, 0);
            assertThat(records, hasSize(2));
            assertThat(records.get(0).getOperation(), is(ShuffleRecord.OP_INSERT));
            assertThat(records.get(0).getSerializedRecord(), equalTo(data1));
            assertThat(records.get(1).getOperation(), is(ShuffleRecord.OP_DELETE));
            assertThat(records.get(1).getSerializedRecord(), equalTo(data2));
            assertThat(records.get(1).getChangeOrdinal(), is(0));
        }
    }

    @Test
    void roundTrip_multiplePartitions_recordsSortedByPartition() throws Exception {
        try (ShuffleWriter writer = storage.createWriter("snap1", "task1", 4)) {
            // Add records out of partition order
            writer.addRecord(2, ShuffleRecord.OP_INSERT, 0, new byte[]{20});
            writer.addRecord(0, ShuffleRecord.OP_INSERT, 0, new byte[]{1});
            writer.addRecord(3, ShuffleRecord.OP_DELETE, 1, new byte[]{30});
            writer.addRecord(0, ShuffleRecord.OP_DELETE, 0, new byte[]{2});
            writer.addRecord(2, ShuffleRecord.OP_DELETE, 0, new byte[]{21});
            writer.finish();
        }

        try (ShuffleReader reader = storage.createReader("snap1", "task1")) {
            // Partition 0: two records
            final List<ShuffleRecord> p0 = reader.readPartitions(0, 0);
            assertThat(p0, hasSize(2));
            assertThat(p0.get(0).getSerializedRecord(), equalTo(new byte[]{1}));
            assertThat(p0.get(1).getSerializedRecord(), equalTo(new byte[]{2}));

            // Partition 1: empty
            final List<ShuffleRecord> p1 = reader.readPartitions(1, 1);
            assertThat(p1, is(empty()));

            // Partition 2: two records
            final List<ShuffleRecord> p2 = reader.readPartitions(2, 2);
            assertThat(p2, hasSize(2));
            assertThat(p2.get(0).getSerializedRecord(), equalTo(new byte[]{20}));

            // Partition 3: one record
            final List<ShuffleRecord> p3 = reader.readPartitions(3, 3);
            assertThat(p3, hasSize(1));
            assertThat(p3.get(0).getChangeOrdinal(), is(1));
        }
    }

    @Test
    void roundTrip_readPartitionRange() throws Exception {
        try (ShuffleWriter writer = storage.createWriter("snap1", "task1", 4)) {
            writer.addRecord(0, ShuffleRecord.OP_INSERT, 0, new byte[]{1});
            writer.addRecord(1, ShuffleRecord.OP_INSERT, 0, new byte[]{10});
            writer.addRecord(2, ShuffleRecord.OP_INSERT, 0, new byte[]{20});
            writer.addRecord(3, ShuffleRecord.OP_INSERT, 0, new byte[]{30});
            writer.finish();
        }

        try (ShuffleReader reader = storage.createReader("snap1", "task1")) {
            // Read partitions 1-2 as a range
            final List<ShuffleRecord> range = reader.readPartitions(1, 2);
            assertThat(range, hasSize(2));
            assertThat(range.get(0).getSerializedRecord(), equalTo(new byte[]{10}));
            assertThat(range.get(1).getSerializedRecord(), equalTo(new byte[]{20}));
        }
    }

    @Test
    void emptyPartitions_indexOffsetsCorrect() throws Exception {
        try (ShuffleWriter writer = storage.createWriter("snap1", "task1", 5)) {
            // Only partition 2 has data
            writer.addRecord(2, ShuffleRecord.OP_INSERT, 0, new byte[]{99});
            writer.finish();
        }

        try (ShuffleReader reader = storage.createReader("snap1", "task1")) {
            final long[] offsets = reader.readIndex();
            assertThat(offsets.length, is(6)); // 5 partitions + 1

            // Partitions 0, 1 are empty
            assertThat(offsets[0], is(0L));
            assertThat(offsets[1], is(0L));
            assertThat(offsets[2], is(0L));

            // Partition 2 has data
            assertThat(offsets[3] > offsets[2], is(true));

            // Partitions 3, 4 are empty
            assertThat(offsets[4], is(offsets[3]));
            assertThat(offsets[5], is(offsets[3]));

            // Reading empty partitions returns empty list
            assertThat(reader.readPartitions(0, 0), is(empty()));
            assertThat(reader.readPartitions(3, 4), is(empty()));

            // Reading partition 2 returns the record
            final List<ShuffleRecord> p2 = reader.readPartitions(2, 2);
            assertThat(p2, hasSize(1));
            assertThat(p2.get(0).getSerializedRecord(), equalTo(new byte[]{99}));
        }
    }

    @Test
    void readBytes_returnsCorrectRange() throws Exception {
        try (ShuffleWriter writer = storage.createWriter("snap1", "task1", 2)) {
            writer.addRecord(0, ShuffleRecord.OP_INSERT, 0, new byte[]{1, 2, 3});
            writer.addRecord(1, ShuffleRecord.OP_INSERT, 0, new byte[]{4, 5, 6});
            writer.finish();
        }

        try (ShuffleReader reader = storage.createReader("snap1", "task1")) {
            final long[] offsets = reader.readIndex();
            // Read only partition 1's compressed block
            final long offset = offsets[1];
            final int length = (int) (offsets[2] - offsets[1]);
            final byte[] compressedBlock = reader.readBytes(offset, length);

            // Decompress and parse
            final byte[] uncompressed = LocalDiskShuffleReader.decompressBlock(compressedBlock);
            final List<ShuffleRecord> records = LocalDiskShuffleReader.parseRecords(uncompressed);
            assertThat(records, hasSize(1));
            assertThat(records.get(0).getSerializedRecord(), equalTo(new byte[]{4, 5, 6}));
        }
    }
}
