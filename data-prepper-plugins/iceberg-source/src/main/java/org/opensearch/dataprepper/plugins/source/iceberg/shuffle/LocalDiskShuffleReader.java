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

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads LZ4-compressed shuffle data from a data file + index file written by {@link LocalDiskShuffleWriter}.
 */
public class LocalDiskShuffleReader implements ShuffleReader {

    private static final LZ4FastDecompressor DECOMPRESSOR = LZ4Factory.fastestInstance().fastDecompressor();
    private static final int BLOCK_HEADER_SIZE = Integer.BYTES + Integer.BYTES;

    private final Path dataFilePath;
    private final Path indexFilePath;

    LocalDiskShuffleReader(final Path dataFilePath, final Path indexFilePath) {
        this.dataFilePath = dataFilePath;
        this.indexFilePath = indexFilePath;
    }

    @Override
    public long[] readIndex() {
        try {
            final byte[] indexBytes = Files.readAllBytes(indexFilePath);
            final ByteBuffer buf = ByteBuffer.wrap(indexBytes);
            final long[] offsets = new long[indexBytes.length / Long.BYTES];
            for (int i = 0; i < offsets.length; i++) {
                offsets[i] = buf.getLong();
            }
            return offsets;
        } catch (final IOException e) {
            throw new UncheckedIOException("Failed to read shuffle index: " + indexFilePath, e);
        }
    }

    @Override
    public List<ShuffleRecord> readPartitions(final int startPartition, final int endPartitionInclusive) {
        final long[] offsets = readIndex();
        final List<ShuffleRecord> allRecords = new ArrayList<>();

        for (int p = startPartition; p <= endPartitionInclusive; p++) {
            final long start = offsets[p];
            final long end = offsets[p + 1];
            if (start == end) {
                continue;
            }
            final byte[] compressedBlock = readBytes(start, (int) (end - start));
            final byte[] uncompressed = decompressBlock(compressedBlock);
            allRecords.addAll(parseRecords(uncompressed));
        }

        return allRecords;
    }

    @Override
    public byte[] readBytes(final long offset, final int length) {
        if (length == 0) {
            return new byte[0];
        }
        try (RandomAccessFile raf = new RandomAccessFile(dataFilePath.toFile(), "r")) {
            raf.seek(offset);
            final byte[] data = new byte[length];
            raf.readFully(data);
            return data;
        } catch (final IOException e) {
            throw new UncheckedIOException("Failed to read shuffle data: " + dataFilePath, e);
        }
    }

    @Override
    public void close() {
        // No persistent resources to close
    }

    /**
     * Decompresses a block: [4 bytes: uncompressed size][4 bytes: compressed size][compressed data]
     */
    public static byte[] decompressBlock(final byte[] block) {
        final ByteBuffer buf = ByteBuffer.wrap(block);
        final int uncompressedSize = buf.getInt();
        final int compressedSize = buf.getInt();
        final byte[] uncompressed = new byte[uncompressedSize];
        DECOMPRESSOR.decompress(block, BLOCK_HEADER_SIZE, uncompressed, 0, uncompressedSize);
        return uncompressed;
    }

    public static List<ShuffleRecord> parseRecords(final byte[] data) {
        final List<ShuffleRecord> records = new ArrayList<>();
        final ByteBuffer buf = ByteBuffer.wrap(data);
        while (buf.hasRemaining()) {
            final int recordLength = buf.getInt();
            final byte operation = buf.get();
            final int changeOrdinal = buf.getInt();
            final byte[] serialized = new byte[recordLength - ShuffleRecord.OPERATION_SIZE - ShuffleRecord.CHANGE_ORDINAL_SIZE];
            buf.get(serialized);
            records.add(new ShuffleRecord(operation, changeOrdinal, serialized));
        }
        return records;
    }
}
