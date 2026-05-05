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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DefaultFileSystemOperationsTest {

    @TempDir
    Path tempDir;

    private DefaultFileSystemOperations fileOps;

    @BeforeEach
    void setUp() {
        fileOps = new DefaultFileSystemOperations();
    }

    @Test
    void openReadChannel_returns_readable_channel() throws IOException {
        final Path file = tempDir.resolve("readable.txt");
        Files.writeString(file, "hello world");

        try (FileChannel channel = fileOps.openReadChannel(file)) {
            assertThat(channel, notNullValue());
            assertThat(channel.isOpen(), equalTo(true));
            assertThat(channel.size(), greaterThan(0L));
        }
    }

    @Test
    void openReadChannel_throws_on_nonexistent_file() {
        final Path missing = tempDir.resolve("does-not-exist.txt");
        assertThrows(IOException.class, () -> fileOps.openReadChannel(missing));
    }

    @Test
    void readAttributes_returns_valid_attributes() throws IOException {
        final Path file = tempDir.resolve("attrs.txt");
        Files.writeString(file, "test content");

        final BasicFileAttributes attrs = fileOps.readAttributes(file);

        assertThat(attrs, notNullValue());
        assertThat(attrs.isRegularFile(), equalTo(true));
        assertThat(attrs.isDirectory(), equalTo(false));
        assertThat(attrs.size(), greaterThan(0L));
        assertThat(attrs.creationTime(), notNullValue());
    }

    @Test
    void readAttributes_throws_on_nonexistent_file() {
        final Path missing = tempDir.resolve("no-attrs.txt");
        assertThrows(IOException.class, () -> fileOps.readAttributes(missing));
    }

    @Test
    void listDirectory_returns_files_in_directory() throws IOException {
        Files.createFile(tempDir.resolve("file1.txt"));
        Files.createFile(tempDir.resolve("file2.txt"));

        try (Stream<Path> stream = fileOps.listDirectory(tempDir)) {
            final List<Path> files = stream.collect(Collectors.toList());
            assertThat(files.size(), greaterThanOrEqualTo(2));
            assertThat(files, hasItem(tempDir.resolve("file1.txt")));
            assertThat(files, hasItem(tempDir.resolve("file2.txt")));
        }
    }

    @Test
    void listDirectory_throws_on_nonexistent_directory() {
        final Path missing = tempDir.resolve("no-dir");
        assertThrows(IOException.class, () -> fileOps.listDirectory(missing));
    }

    @Test
    void exists_returns_true_for_existing_file() throws IOException {
        final Path file = tempDir.resolve("exists.txt");
        Files.writeString(file, "data");

        assertThat(fileOps.exists(file), equalTo(true));
    }

    @Test
    void exists_returns_false_for_nonexistent_file() {
        final Path missing = tempDir.resolve("missing.txt");
        assertThat(fileOps.exists(missing), equalTo(false));
    }

    @Test
    void size_returns_correct_file_size() throws IOException {
        final Path file = tempDir.resolve("sized.txt");
        final String content = "twelve chars";
        Files.writeString(file, content);

        assertThat(fileOps.size(file), equalTo((long) content.getBytes().length));
    }

    @Test
    void size_returns_zero_for_empty_file() throws IOException {
        final Path file = tempDir.resolve("empty.txt");
        Files.createFile(file);

        assertThat(fileOps.size(file), equalTo(0L));
    }

    @Test
    void size_throws_on_nonexistent_file() {
        final Path missing = tempDir.resolve("no-size.txt");
        assertThrows(IOException.class, () -> fileOps.size(missing));
    }

    @Test
    void readBytes_reads_exact_number_of_bytes() throws IOException {
        final Path file = tempDir.resolve("bytes.txt");
        Files.writeString(file, "abcdefghij");

        final byte[] bytes = fileOps.readBytes(file, 5);

        assertThat(bytes, notNullValue());
        assertThat(bytes.length, equalTo(5));
        assertThat(new String(bytes), equalTo("abcde"));
    }

    @Test
    void readBytes_reads_entire_file_when_length_exceeds_size() throws IOException {
        final Path file = tempDir.resolve("short.txt");
        Files.writeString(file, "abc");

        final byte[] bytes = fileOps.readBytes(file, 100);

        assertThat(bytes.length, equalTo(3));
        assertThat(new String(bytes), equalTo("abc"));
    }

    @Test
    void readBytes_returns_empty_array_for_empty_file() throws IOException {
        final Path file = tempDir.resolve("empty-bytes.txt");
        Files.createFile(file);

        final byte[] bytes = fileOps.readBytes(file, 10);

        assertThat(bytes, notNullValue());
        assertThat(bytes.length, equalTo(0));
    }

    @Test
    void readBytes_throws_on_nonexistent_file() {
        final Path missing = tempDir.resolve("no-bytes.txt");
        assertThrows(IOException.class, () -> fileOps.readBytes(missing, 10));
    }
}
