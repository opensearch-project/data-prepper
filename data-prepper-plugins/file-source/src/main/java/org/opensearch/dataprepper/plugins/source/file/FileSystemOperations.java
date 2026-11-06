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
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.stream.Stream;

public interface FileSystemOperations {

    FileChannel openReadChannel(Path path) throws IOException;

    BasicFileAttributes readAttributes(Path path) throws IOException;

    Stream<Path> listDirectory(Path directory) throws IOException;

    boolean exists(Path path);

    long size(Path path) throws IOException;

    byte[] readBytes(Path path, int length) throws IOException;
}
