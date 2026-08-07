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

import java.nio.file.Path;
import java.util.Objects;

public final class PendingFile {

    private final FileIdentity fileIdentity;
    private final Path path;
    private final long enqueuedTimeMillis;

    public PendingFile(final FileIdentity fileIdentity, final Path path) {
        this.fileIdentity = Objects.requireNonNull(fileIdentity, "fileIdentity must not be null");
        this.path = Objects.requireNonNull(path, "path must not be null");
        this.enqueuedTimeMillis = System.currentTimeMillis();
    }

    public FileIdentity getFileIdentity() {
        return fileIdentity;
    }

    public Path getPath() {
        return path;
    }

    public long getEnqueuedTimeMillis() {
        return enqueuedTimeMillis;
    }

    @Override
    public String toString() {
        return "PendingFile{path=" + path + ", identity=" + fileIdentity + "}";
    }
}
