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
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.zip.CRC32;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
class FileIdentityTest {

    private static final int FINGERPRINT_BYTES = 256;

    @TempDir
    Path tempDir;

    @Mock
    private FileSystemOperations fileOps;

    @Mock
    private BasicFileAttributes attrs;

    @Test
    void fromReturnsInodeBasedIdentityWhenFileKeyPresent() throws IOException {
        final Path testFile = tempDir.resolve("test.log");
        Files.writeString(testFile, "some content");

        final Object fileKey = "12345";
        when(fileOps.readAttributes(testFile)).thenReturn(attrs);
        when(attrs.fileKey()).thenReturn(fileKey);

        final FileIdentity identity = FileIdentity.from(testFile, fileOps, FINGERPRINT_BYTES);

        assertThat(identity, notNullValue());
        assertThat(identity.toString(), containsString("inode:12345"));
        assertThat(identity.getPath(), equalTo(testFile));
    }

    @Test
    void fromReturnsCrcFallbackWhenFileKeyIsNull() throws IOException {
        final Path testFile = tempDir.resolve("test.log");
        final String content = "hello world data";
        Files.writeString(testFile, content);
        final byte[] contentBytes = content.getBytes();

        when(fileOps.readAttributes(testFile)).thenReturn(attrs);
        when(attrs.fileKey()).thenReturn(null);
        when(fileOps.size(testFile)).thenReturn((long) contentBytes.length);
        when(fileOps.readBytes(testFile, Math.min(FINGERPRINT_BYTES, contentBytes.length))).thenReturn(contentBytes);
        when(attrs.creationTime()).thenReturn(FileTime.from(Instant.parse("2025-01-01T00:00:00Z")));

        final CRC32 expectedCrc = new CRC32();
        expectedCrc.update(contentBytes);

        final FileIdentity identity = FileIdentity.from(testFile, fileOps, FINGERPRINT_BYTES);

        assertThat(identity, notNullValue());
        assertThat(identity.toString(), containsString("crc:" + expectedCrc.getValue()));
        assertThat(identity.toString(), containsString("size:" + contentBytes.length));
    }

    @Test
    void fromReturnsPathFallbackWhenIOExceptionOnReadAttributes() throws IOException {
        final Path testFile = tempDir.resolve("missing.log");

        when(fileOps.readAttributes(testFile)).thenThrow(new IOException("cannot read"));

        final FileIdentity identity = FileIdentity.from(testFile, fileOps, FINGERPRINT_BYTES);

        assertThat(identity, notNullValue());
        assertThat(identity.toString(), containsString("path:"));
        assertThat(identity.toString(), containsString(testFile.toAbsolutePath().toString()));
    }

    @Test
    void fromReturnsPathFallbackForEmptyFile() throws IOException {
        final Path testFile = tempDir.resolve("empty.log");
        Files.writeString(testFile, "");

        when(fileOps.readAttributes(testFile)).thenReturn(attrs);
        when(attrs.fileKey()).thenReturn(null);
        when(fileOps.size(testFile)).thenReturn(0L);

        final FileIdentity identity = FileIdentity.from(testFile, fileOps, FINGERPRINT_BYTES);

        assertThat(identity, notNullValue());
        assertThat(identity.toString(), containsString("path:"));
    }

    @Test
    void fromReturnsPathFallbackWhenSizeThrowsIOException() throws IOException {
        final Path testFile = tempDir.resolve("unreadable.log");

        when(fileOps.readAttributes(testFile)).thenReturn(attrs);
        when(attrs.fileKey()).thenReturn(null);
        when(fileOps.size(testFile)).thenThrow(new IOException("permission denied"));

        final FileIdentity identity = FileIdentity.from(testFile, fileOps, FINGERPRINT_BYTES);

        assertThat(identity, notNullValue());
        assertThat(identity.toString(), containsString("path:"));
    }

    @Test
    void equalIdentitiesAreEqual() throws IOException {
        final Path testFile = tempDir.resolve("a.log");
        Files.writeString(testFile, "data");

        final Object fileKey = "inode-99";
        when(fileOps.readAttributes(testFile)).thenReturn(attrs);
        when(attrs.fileKey()).thenReturn(fileKey);

        final FileIdentity first = FileIdentity.from(testFile, fileOps, FINGERPRINT_BYTES);
        final FileIdentity second = FileIdentity.from(testFile, fileOps, FINGERPRINT_BYTES);

        assertThat(first, equalTo(second));
        assertThat(first.hashCode(), equalTo(second.hashCode()));
    }

    @Test
    void differentIdentitiesAreNotEqual() throws IOException {
        final Path fileA = tempDir.resolve("a.log");
        final Path fileB = tempDir.resolve("b.log");
        Files.writeString(fileA, "data-a");
        Files.writeString(fileB, "data-b");

        @SuppressWarnings("unchecked")
        final BasicFileAttributes attrsB = mock(BasicFileAttributes.class);

        when(fileOps.readAttributes(fileA)).thenReturn(attrs);
        when(attrs.fileKey()).thenReturn("inode-1");

        when(fileOps.readAttributes(fileB)).thenReturn(attrsB);
        when(attrsB.fileKey()).thenReturn("inode-2");

        final FileIdentity identityA = FileIdentity.from(fileA, fileOps, FINGERPRINT_BYTES);
        final FileIdentity identityB = FileIdentity.from(fileB, fileOps, FINGERPRINT_BYTES);

        assertThat(identityA, not(equalTo(identityB)));
    }

    @Test
    void identityIsNotEqualToNull() throws IOException {
        final Path testFile = tempDir.resolve("file.log");
        Files.writeString(testFile, "content");

        when(fileOps.readAttributes(testFile)).thenReturn(attrs);
        when(attrs.fileKey()).thenReturn("inode-42");

        final FileIdentity identity = FileIdentity.from(testFile, fileOps, FINGERPRINT_BYTES);

        assertThat(identity.equals(null), equalTo(false));
    }

    @Test
    void identityIsNotEqualToDifferentClass() throws IOException {
        final Path testFile = tempDir.resolve("diffclass.log");
        Files.writeString(testFile, "content");

        when(fileOps.readAttributes(testFile)).thenReturn(attrs);
        when(attrs.fileKey()).thenReturn("inode-99");

        final FileIdentity identity = FileIdentity.from(testFile, fileOps, FINGERPRINT_BYTES);

        assertThat(identity.equals("a string object"), equalTo(false));
    }

    @Test
    void identityIsEqualToItself() throws IOException {
        final Path testFile = tempDir.resolve("self.log");
        Files.writeString(testFile, "content");

        when(fileOps.readAttributes(testFile)).thenReturn(attrs);
        when(attrs.fileKey()).thenReturn("inode-7");

        final FileIdentity identity = FileIdentity.from(testFile, fileOps, FINGERPRINT_BYTES);

        assertThat(identity.equals(identity), equalTo(true));
    }

    @Test
    void getPathReturnsOriginalPath() throws IOException {
        final Path testFile = tempDir.resolve("path-check.log");
        Files.writeString(testFile, "content");

        when(fileOps.readAttributes(testFile)).thenReturn(attrs);
        when(attrs.fileKey()).thenReturn("inode-10");

        final FileIdentity identity = FileIdentity.from(testFile, fileOps, FINGERPRINT_BYTES);

        assertThat(identity.getPath(), equalTo(testFile));
    }

    @Test
    void crcFallbackUsesCreationTime() throws IOException {
        final Path testFile = tempDir.resolve("created.log");
        final String content = "log data here";
        Files.writeString(testFile, content);
        final byte[] bytes = content.getBytes();

        when(fileOps.readAttributes(testFile)).thenReturn(attrs);
        when(attrs.fileKey()).thenReturn(null);
        when(fileOps.size(testFile)).thenReturn((long) bytes.length);
        when(fileOps.readBytes(testFile, Math.min(FINGERPRINT_BYTES, bytes.length))).thenReturn(bytes);
        when(attrs.creationTime()).thenReturn(FileTime.from(Instant.parse("2024-06-15T12:30:00Z")));

        final FileIdentity identity = FileIdentity.from(testFile, fileOps, FINGERPRINT_BYTES);

        assertThat(identity.toString(), containsString("created:"));
    }

    @Test
    void crcFallbackHandlesReadBytesIOException() throws IOException {
        final Path testFile = tempDir.resolve("read-fail.log");
        Files.writeString(testFile, "content");

        when(fileOps.readAttributes(testFile)).thenReturn(attrs);
        when(attrs.fileKey()).thenReturn(null);
        when(fileOps.size(testFile)).thenReturn(100L);
        when(fileOps.readBytes(testFile, 100)).thenThrow(new IOException("read failed"));

        final FileIdentity identity = FileIdentity.from(testFile, fileOps, FINGERPRINT_BYTES);

        assertThat(identity, notNullValue());
        assertThat(identity.toString(), containsString("path:"));
    }

    @Test
    void fromThrowsIllegalArgumentExceptionWhenFingerprintBytesIsZero() {
        final Path testFile = tempDir.resolve("zero-fp.log");

        assertThrows(IllegalArgumentException.class,
                () -> FileIdentity.from(testFile, fileOps, 0));
    }

    @Test
    void fromThrowsIllegalArgumentExceptionWhenFingerprintBytesIsNegative() {
        final Path testFile = tempDir.resolve("neg-fp.log");

        assertThrows(IllegalArgumentException.class,
                () -> FileIdentity.from(testFile, fileOps, -1));
    }

    @Test
    void fingerprintBytesLimitsReadSize() throws IOException {
        final Path testFile = tempDir.resolve("large.log");
        Files.writeString(testFile, "a]".repeat(500));

        final int smallFingerprint = 16;

        when(fileOps.readAttributes(testFile)).thenReturn(attrs);
        when(attrs.fileKey()).thenReturn(null);
        when(fileOps.size(testFile)).thenReturn(1000L);
        when(fileOps.readBytes(testFile, smallFingerprint)).thenReturn(new byte[smallFingerprint]);
        when(attrs.creationTime()).thenReturn(FileTime.from(Instant.parse("2025-01-01T00:00:00Z")));

        final FileIdentity identity = FileIdentity.from(testFile, fileOps, smallFingerprint);

        assertThat(identity, notNullValue());
        assertThat(identity.toString(), containsString("crc:"));
    }
}
