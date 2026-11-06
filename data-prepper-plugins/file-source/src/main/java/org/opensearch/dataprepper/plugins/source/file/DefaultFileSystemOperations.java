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
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.stream.Stream;

final class DefaultFileSystemOperations implements FileSystemOperations {

    @Override
    public FileChannel openReadChannel(final Path path) throws IOException {
        return FileChannel.open(path, StandardOpenOption.READ);
    }

    @Override
    public BasicFileAttributes readAttributes(final Path path) throws IOException {
        return Files.readAttributes(path, BasicFileAttributes.class);
    }

    @Override
    public Stream<Path> listDirectory(final Path directory) throws IOException {
        return Files.list(directory);
    }

    @Override
    public boolean exists(final Path path) {
        return Files.exists(path);
    }

    @Override
    public long size(final Path path) throws IOException {
        return Files.size(path);
    }

    @Override
    public byte[] readBytes(final Path path, final int length) throws IOException {
        try (final var inputStream = Files.newInputStream(path)) {
            return inputStream.readNBytes(length);
        }
    }
}
