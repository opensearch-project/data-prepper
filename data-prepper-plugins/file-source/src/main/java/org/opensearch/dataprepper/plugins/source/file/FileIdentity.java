/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.file;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;
import java.util.zip.CRC32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FileIdentity {

    private static final Logger LOG = LoggerFactory.getLogger(FileIdentity.class);
    private final String identity;
    private final Path path;

    private FileIdentity(final String identity, final Path path) {
        this.identity = Objects.requireNonNull(identity, "identity must not be null");
        this.path = Objects.requireNonNull(path, "path must not be null");
    }

    public static FileIdentity from(final Path path, final FileSystemOperations fileOps, final int fingerprintBytes) {
        Objects.requireNonNull(path, "path must not be null");
        Objects.requireNonNull(fileOps, "fileOps must not be null");
        if (fingerprintBytes <= 0) {
            throw new IllegalArgumentException("fingerprintBytes must be positive");
        }

        try {
            final BasicFileAttributes attrs = fileOps.readAttributes(path);
            final Object fileKey = attrs.fileKey();
            if (fileKey != null) {
                return new FileIdentity("inode:" + fileKey, path);
            }
            return buildFallbackIdentity(path, fileOps, attrs, fingerprintBytes);
        } catch (final IOException e) {
            LOG.warn("Failed to read file attributes for {}. Using path-based identity which disables rotation detection.", path, e);
            return new FileIdentity("path:" + path.toAbsolutePath(), path);
        }
    }

    private static FileIdentity buildFallbackIdentity(final Path path, final FileSystemOperations fileOps,
                                                       final BasicFileAttributes attrs, final int fingerprintBytes) {
        long fileSize;
        try {
            fileSize = fileOps.size(path);
        } catch (final IOException e) {
            LOG.warn("Failed to read file size for {}. Using path-based identity.", path, e);
            return new FileIdentity("path:" + path.toAbsolutePath(), path);
        }

        if (fileSize == 0) {
            return new FileIdentity("path:" + path.toAbsolutePath(), path);
        }

        final int bytesToRead = (int) Math.min(fingerprintBytes, fileSize);
        long crcValue = 0;
        try {
            final byte[] bytes = fileOps.readBytes(path, bytesToRead);
            final CRC32 crc32 = new CRC32();
            crc32.update(bytes);
            crcValue = crc32.getValue();
        } catch (final IOException e) {
            LOG.warn("Failed to read fingerprint bytes for {}. Using path-based identity.", path, e);
            return new FileIdentity("path:" + path.toAbsolutePath(), path);
        }

        final String creationTime = attrs.creationTime().toString();
        final String id = "crc:" + crcValue + ":size:" + fileSize + ":created:" + creationTime;
        return new FileIdentity(id, path);
    }

    public Path getPath() {
        return path;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final FileIdentity that = (FileIdentity) o;
        return identity.equals(that.identity);
    }

    @Override
    public int hashCode() {
        return identity.hashCode();
    }

    @Override
    public String toString() {
        return "FileIdentity{" + identity + ", path=" + path + "}";
    }
}
