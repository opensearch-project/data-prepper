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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Objects;

public final class RotationDetector {

    private static final Logger LOG = LoggerFactory.getLogger(RotationDetector.class);

    private final FileSystemOperations fileOps;
    private final int fingerprintBytes;

    public RotationDetector(final FileSystemOperations fileOps, final int fingerprintBytes) {
        this.fileOps = Objects.requireNonNull(fileOps, "fileOps must not be null");
        if (fingerprintBytes <= 0) {
            throw new IllegalArgumentException("fingerprintBytes must be positive");
        }
        this.fingerprintBytes = fingerprintBytes;
    }

    public int getFingerprintBytes() {
        return fingerprintBytes;
    }

    public RotationResult checkRotation(final Path path, final FileIdentity knownIdentity, final long currentOffset) {
        if (!fileOps.exists(path)) {
            LOG.debug("File deleted: {}", path);
            return RotationResult.DELETED;
        }

        final FileIdentity currentIdentity;
        try {
            currentIdentity = FileIdentity.from(path, fileOps, fingerprintBytes);
        } catch (final RuntimeException e) {
            if (isCausedByNoSuchFile(e)) {
                LOG.debug("File deleted: {}", path);
                return RotationResult.DELETED;
            }
            LOG.warn("Error checking rotation for file {}", path, e);
            return RotationResult.NO_ROTATION;
        }

        if (!currentIdentity.equals(knownIdentity)) {
            LOG.info("Create/rename rotation detected for file {}", path);
            return new RotationResult(RotationType.CREATE_RENAME, currentIdentity);
        }

        try {
            final long fileSize = fileOps.size(path);
            if (fileSize < currentOffset) {
                LOG.info("Copytruncate rotation detected for file {} (size={}, offset={})", path, fileSize, currentOffset);
                return new RotationResult(RotationType.COPYTRUNCATE, currentIdentity);
            }
        } catch (final NoSuchFileException e) {
            LOG.debug("File deleted during size check: {}", path);
            return RotationResult.DELETED;
        } catch (final IOException e) {
            LOG.warn("Error reading file size for {}", path, e);
            return RotationResult.NO_ROTATION;
        }

        return RotationResult.NO_ROTATION;
    }

    boolean isCausedByNoSuchFile(final Exception e) {
        if (e instanceof NoSuchFileException) {
            return true;
        }
        Throwable cause = e.getCause();
        while (cause != null) {
            if (cause instanceof NoSuchFileException) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }
}
