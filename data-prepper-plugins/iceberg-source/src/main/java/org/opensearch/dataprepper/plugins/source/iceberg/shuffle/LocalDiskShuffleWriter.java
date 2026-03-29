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

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Buffers records in memory, sorts by partition number, and writes a single data file + index file.
 * Each partition's data block is LZ4 compressed.
 * <p>
 * Data file format: for each non-empty partition, a compressed block:
 *   [4 bytes: uncompressed size][4 bytes: compressed size][compressed data]
 * <p>
 * Within each uncompressed block, records are:
 *   [4 bytes: record length][1 byte: operation][4 bytes: changeOrdinal][N bytes: serialized data]
 * <p>
 * Index file format: (numPartitions + 1) long values (8 bytes each) representing byte offsets
 * into the data file. offset[i] to offset[i+1] is the byte range for partition i.
 */
class LocalDiskShuffleWriter implements ShuffleWriter {

    private static final LZ4Factory LZ4 = LZ4Factory.fastestInstance();

    private final Path dataFilePath;
    private final Path indexFilePath;
    private final int numPartitions;
    private final List<BufferedRecord> buffer = new ArrayList<>();

    LocalDiskShuffleWriter(final Path dataFilePath, final Path indexFilePath, final int numPartitions) {
        this.dataFilePath = dataFilePath;
        this.indexFilePath = indexFilePath;
        this.numPartitions = numPartitions;
    }

    @Override
    public void addRecord(final int partitionNumber, final byte operation, final int changeOrdinal,
                          final byte[] serializedRecord) {
        buffer.add(new BufferedRecord(partitionNumber, operation, changeOrdinal, serializedRecord));
    }

    @Override
    public void finish() {
        buffer.sort(Comparator.comparingInt(r -> r.partitionNumber));

        final long[] offsets = new long[numPartitions + 1];
        long currentOffset = 0;

        // Group records by partition
        final List<List<BufferedRecord>> partitionGroups = new ArrayList<>();
        for (int i = 0; i < numPartitions; i++) {
            partitionGroups.add(new ArrayList<>());
        }
        for (final BufferedRecord record : buffer) {
            partitionGroups.get(record.partitionNumber).add(record);
        }

        try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(dataFilePath))) {
            final LZ4Compressor compressor = LZ4.fastCompressor();

            for (int p = 0; p < numPartitions; p++) {
                offsets[p] = currentOffset;
                final List<BufferedRecord> records = partitionGroups.get(p);
                if (records.isEmpty()) {
                    continue;
                }

                // Serialize records to uncompressed bytes
                final byte[] uncompressed = serializeRecords(records);

                // Compress
                final int maxCompressedLength = compressor.maxCompressedLength(uncompressed.length);
                final byte[] compressed = new byte[maxCompressedLength];
                final int compressedLength = compressor.compress(uncompressed, 0, uncompressed.length,
                        compressed, 0, maxCompressedLength);

                // Write: [uncompressed size][compressed size][compressed data]
                final byte[] header = new byte[8];
                ByteBuffer.wrap(header).putInt(uncompressed.length).putInt(compressedLength);
                out.write(header);
                out.write(compressed, 0, compressedLength);
                currentOffset += 8 + compressedLength;
            }
            offsets[numPartitions] = currentOffset;
        } catch (final IOException e) {
            throw new UncheckedIOException("Failed to write shuffle data file: " + dataFilePath, e);
        }

        // Write index file
        try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(indexFilePath))) {
            final ByteBuffer indexBuffer = ByteBuffer.allocate((numPartitions + 1) * Long.BYTES);
            for (final long offset : offsets) {
                indexBuffer.putLong(offset);
            }
            out.write(indexBuffer.array());
        } catch (final IOException e) {
            throw new UncheckedIOException("Failed to write shuffle index file: " + indexFilePath, e);
        }

        buffer.clear();
    }

    @Override
    public void close() {
        buffer.clear();
    }

    private static byte[] serializeRecords(final List<BufferedRecord> records) {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (final BufferedRecord record : records) {
            final int recordLength = 1 + 4 + record.serializedRecord.length;
            final byte[] header = new byte[4 + 1 + 4];
            ByteBuffer.wrap(header)
                    .putInt(recordLength)
                    .put(record.operation)
                    .putInt(record.changeOrdinal);
            try {
                baos.write(header);
                baos.write(record.serializedRecord);
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return baos.toByteArray();
    }

    private static class BufferedRecord {
        final int partitionNumber;
        final byte operation;
        final int changeOrdinal;
        final byte[] serializedRecord;

        BufferedRecord(final int partitionNumber, final byte operation, final int changeOrdinal,
                       final byte[] serializedRecord) {
            this.partitionNumber = partitionNumber;
            this.operation = operation;
            this.changeOrdinal = changeOrdinal;
            this.serializedRecord = serializedRecord;
        }
    }
}
