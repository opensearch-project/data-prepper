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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Local disk implementation of {@link ShuffleStorage}.
 * Files are stored under {baseDir}/{snapshotId}/shuffle-{taskId}.data and .index.
 */
public class LocalDiskShuffleStorage implements ShuffleStorage {

    private static final Logger LOG = LoggerFactory.getLogger(LocalDiskShuffleStorage.class);
    private static final String DATA_SUFFIX = ".data";
    private static final String INDEX_SUFFIX = ".index";

    private final Path baseDir;

    public LocalDiskShuffleStorage(final Path baseDir) {
        this.baseDir = baseDir;
    }

    Path dataFilePath(final String snapshotId, final String taskId) {
        final Path snapshotDir = validateSubdirectory(baseDir.resolve(snapshotId));
        return validateSubdirectory(snapshotDir.resolve("shuffle-" + taskId + DATA_SUFFIX));
    }

    Path indexFilePath(final String snapshotId, final String taskId) {
        final Path snapshotDir = validateSubdirectory(baseDir.resolve(snapshotId));
        return validateSubdirectory(snapshotDir.resolve("shuffle-" + taskId + INDEX_SUFFIX));
    }

    @Override
    public ShuffleWriter createWriter(final String snapshotId, final String taskId, final int numPartitions) {
        final Path snapshotDir = validateSubdirectory(baseDir.resolve(snapshotId));
        try {
            Files.createDirectories(snapshotDir);
        } catch (final IOException e) {
            throw new UncheckedIOException("Failed to create shuffle directory: " + snapshotDir, e);
        }
        return new LocalDiskShuffleWriter(
                dataFilePath(snapshotId, taskId),
                indexFilePath(snapshotId, taskId),
                numPartitions);
    }

    @Override
    public ShuffleReader createReader(final String snapshotId, final String taskId) {
        return new LocalDiskShuffleReader(
                dataFilePath(snapshotId, taskId),
                indexFilePath(snapshotId, taskId));
    }

    @Override
    public long[] getPartitionSizes(final String snapshotId, final int numPartitions) {
        final long[] sizes = new long[numPartitions];
        for (final String taskId : getTaskIds(snapshotId)) {
            try (ShuffleReader reader = createReader(snapshotId, taskId)) {
                final long[] offsets = reader.readIndex();
                for (int i = 0; i < numPartitions && i + 1 < offsets.length; i++) {
                    sizes[i] += offsets[i + 1] - offsets[i];
                }
            } catch (final IOException e) {
                throw new UncheckedIOException("Failed to read index for task " + taskId, e);
            }
        }
        return sizes;
    }

    @Override
    public List<String> getTaskIds(final String snapshotId) {
        final Path snapshotDir = validateSubdirectory(baseDir.resolve(snapshotId));
        final List<String> taskIds = new ArrayList<>();
        if (!Files.exists(snapshotDir)) {
            return taskIds;
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(snapshotDir, "*" + INDEX_SUFFIX)) {
            for (final Path path : stream) {
                final String fileName = path.getFileName().toString();
                // shuffle-{taskId}.index -> taskId
                taskIds.add(fileName.substring("shuffle-".length(), fileName.length() - INDEX_SUFFIX.length()));
            }
        } catch (final IOException e) {
            throw new UncheckedIOException("Failed to list shuffle files in " + snapshotDir, e);
        }
        return taskIds;
    }

    @Override
    public void cleanup(final String snapshotId) {
        final Path snapshotDir = validateSubdirectory(baseDir.resolve(snapshotId));
        deleteDirectory(snapshotDir);
    }

    @Override
    public void cleanupAll() {
        deleteDirectory(baseDir);
    }

    private Path validateSubdirectory(final Path path) {
        final Path normalized = path.normalize();
        if (!normalized.startsWith(baseDir)) {
            throw new IllegalArgumentException("Invalid path: resolved path is outside the base directory");
        }
        return normalized;
    }
    private void deleteDirectory(final Path dir) {
        if (!Files.exists(dir)) {
            return;
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (final Path entry : stream) {
                if (Files.isDirectory(entry)) {
                    deleteDirectory(entry);
                } else {
                    Files.deleteIfExists(entry);
                }
            }
            Files.deleteIfExists(dir);
        } catch (final IOException e) {
            LOG.warn("Failed to delete shuffle directory: {}", dir, e);
        }
    }
}
