/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.iceberg;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.WriteResult;
import org.opensearch.dataprepper.plugins.sink.iceberg.coordination.state.WriteResultState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.UUID;

/**
 * Writes and reads delta manifest files for transferring WriteResults
 * between worker threads and the CommitScheduler.
 * <p>
 * Data/delete file metadata is stored in manifest files on storage (e.g. S3).
 * The coordination store only holds Base64-encoded ManifestFile pointers.
 */
public class DeltaManifestWriter {

    private static final Logger LOG = LoggerFactory.getLogger(DeltaManifestWriter.class);
    private static final long DUMMY_SNAPSHOT_ID = 0L;

    private final Table table;

    public DeltaManifestWriter(final Table table) {
        this.table = table;
    }

    /**
     * Write a WriteResult as delta manifest files under the table location.
     * Returns a WriteResultState with Base64-encoded ManifestFile metadata.
     */
    public WriteResultState write(final String tableIdentifier, final WriteResult result) throws IOException {
        final String basePath = table.location() + "/metadata/delta-" + UUID.randomUUID();
        final FileIO io = table.io();
        final PartitionSpec spec = table.spec();
        final int formatVersion = TableUtil.formatVersion(table);

        String dataManifestEncoded = null;
        String deleteManifestEncoded = null;

        if (result.dataFiles().length > 0) {
            final String path = basePath + "-data.avro";
            final ManifestFile mf = writeDataManifest(
                    io.newOutputFile(path), spec, formatVersion, result.dataFiles());
            dataManifestEncoded = Base64.getEncoder().encodeToString(ManifestFiles.encode(mf));
        }

        if (result.deleteFiles().length > 0) {
            final String path = basePath + "-delete.avro";
            final ManifestFile mf = writeDeleteManifest(
                    io.newOutputFile(path), spec, formatVersion, result.deleteFiles());
            deleteManifestEncoded = Base64.getEncoder().encodeToString(ManifestFiles.encode(mf));
        }

        LOG.debug("Wrote delta manifests for table {}", table.name());
        return new WriteResultState(tableIdentifier, dataManifestEncoded, deleteManifestEncoded);
    }

    /**
     * Read a WriteResult from delta manifest files referenced by a WriteResultState.
     */
    public WriteResult read(final WriteResultState state) throws IOException {
        final FileIO io = table.io();
        final WriteResult.Builder builder = WriteResult.builder();

        if (state.getDataManifest() != null) {
            final ManifestFile mf = ManifestFiles.decode(
                    Base64.getDecoder().decode(state.getDataManifest()));
            try (CloseableIterable<DataFile> files = ManifestFiles.read(mf, io, table.specs())) {
                builder.addDataFiles(files);
            }
        }

        if (state.getDeleteManifest() != null) {
            final ManifestFile mf = ManifestFiles.decode(
                    Base64.getDecoder().decode(state.getDeleteManifest()));
            try (CloseableIterable<DeleteFile> files =
                         ManifestFiles.readDeleteManifest(mf, io, table.specs())) {
                builder.addDeleteFiles(files);
            }
        }

        return builder.build();
    }

    /**
     * Delete the delta manifest files after a successful commit.
     */
    public void delete(final WriteResultState state) {
        final FileIO io = table.io();
        try {
            if (state.getDataManifest() != null) {
                final ManifestFile mf = ManifestFiles.decode(
                        Base64.getDecoder().decode(state.getDataManifest()));
                io.deleteFile(mf.path());
            }
            if (state.getDeleteManifest() != null) {
                final ManifestFile mf = ManifestFiles.decode(
                        Base64.getDecoder().decode(state.getDeleteManifest()));
                io.deleteFile(mf.path());
            }
        } catch (final IOException e) {
            LOG.warn("Failed to delete delta manifest files", e);
        }
    }

    private ManifestFile writeDataManifest(final OutputFile outputFile, final PartitionSpec spec,
                                           final int formatVersion, final DataFile[] dataFiles) throws IOException {
        final ManifestWriter<DataFile> writer =
                ManifestFiles.write(formatVersion, spec, outputFile, DUMMY_SNAPSHOT_ID);
        try (ManifestWriter<DataFile> w = writer) {
            Arrays.stream(dataFiles).forEach(w::add);
        }
        return writer.toManifestFile();
    }

    private ManifestFile writeDeleteManifest(final OutputFile outputFile, final PartitionSpec spec,
                                             final int formatVersion, final DeleteFile[] deleteFiles) throws IOException {
        final ManifestWriter<DeleteFile> writer =
                ManifestFiles.writeDeleteManifest(formatVersion, spec, outputFile, DUMMY_SNAPSHOT_ID);
        try (ManifestWriter<DeleteFile> w = writer) {
            Arrays.stream(deleteFiles).forEach(w::add);
        }
        return writer.toManifestFile();
    }

}
