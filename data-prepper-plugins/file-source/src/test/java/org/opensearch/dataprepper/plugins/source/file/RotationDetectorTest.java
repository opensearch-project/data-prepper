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
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Instant;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
class RotationDetectorTest {

    private static final int FINGERPRINT_BYTES = 256;

    @TempDir
    Path tempDir;

    @Mock
    private FileSystemOperations fileOps;

    private RotationDetector rotationDetector;

    @BeforeEach
    void setUp() {
        rotationDetector = new RotationDetector(fileOps, FINGERPRINT_BYTES);
        lenient().when(fileOps.exists(any())).thenReturn(true);
    }

    @Test
    void getFingerprintBytesReturnsConfiguredValue() {
        assertThat(rotationDetector.getFingerprintBytes(), equalTo(FINGERPRINT_BYTES));
    }

    @Test
    void noRotationWhenIdentityMatchesAndSizeNotShrunk() throws IOException {
        final Path testFile = tempDir.resolve("app.log");
        Files.writeString(testFile, "log data");

        final BasicFileAttributes attrs = mock(BasicFileAttributes.class);
        when(attrs.fileKey()).thenReturn("inode-42");
        when(fileOps.readAttributes(testFile)).thenReturn(attrs);
        when(fileOps.size(testFile)).thenReturn(100L);

        final FileIdentity knownIdentity = FileIdentity.from(testFile, fileOps, FINGERPRINT_BYTES);

        final RotationResult result = rotationDetector.checkRotation(testFile, knownIdentity, 50L);

        assertThat(result.getRotationType(), equalTo(RotationType.NO_ROTATION));
        assertThat(result.getNewFileIdentity(), nullValue());
    }

    @Test
    void createRenameRotationWhenIdentityChanges() throws IOException {
        final Path testFile = tempDir.resolve("app.log");
        Files.writeString(testFile, "original");

        final BasicFileAttributes oldAttrs = mock(BasicFileAttributes.class);
        when(oldAttrs.fileKey()).thenReturn("inode-1");
        when(fileOps.readAttributes(testFile)).thenReturn(oldAttrs);
        final FileIdentity knownIdentity = FileIdentity.from(testFile, fileOps, FINGERPRINT_BYTES);

        final BasicFileAttributes newAttrs = mock(BasicFileAttributes.class);
        when(newAttrs.fileKey()).thenReturn("inode-2");
        when(fileOps.readAttributes(testFile)).thenReturn(newAttrs);

        final RotationResult result = rotationDetector.checkRotation(testFile, knownIdentity, 100L);

        assertThat(result.getRotationType(), equalTo(RotationType.CREATE_RENAME));
        assertThat(result.getNewFileIdentity(), notNullValue());
    }

    @Test
    void copytruncateRotationWhenFileSizeShrinks() throws IOException {
        final Path testFile = tempDir.resolve("app.log");
        Files.writeString(testFile, "data");

        final BasicFileAttributes attrs = mock(BasicFileAttributes.class);
        when(attrs.fileKey()).thenReturn("inode-5");
        when(fileOps.readAttributes(testFile)).thenReturn(attrs);

        final FileIdentity knownIdentity = FileIdentity.from(testFile, fileOps, FINGERPRINT_BYTES);

        when(fileOps.size(testFile)).thenReturn(10L);

        final RotationResult result = rotationDetector.checkRotation(testFile, knownIdentity, 500L);

        assertThat(result.getRotationType(), equalTo(RotationType.COPYTRUNCATE));
        assertThat(result.getNewFileIdentity(), notNullValue());
    }

    @Test
    void deletedFileWhenWrappedNoSuchFileExceptionFromIdentityResolution() throws IOException {
        final Path testFile = tempDir.resolve("gone.log");
        when(fileOps.exists(testFile)).thenReturn(false);

        final FileIdentity knownIdentity = mock(FileIdentity.class);

        final RotationResult result = rotationDetector.checkRotation(testFile, knownIdentity, 100L);

        assertThat(result.getRotationType(), equalTo(RotationType.DELETED));
    }

    @Test
    void deletedFileWhenSizeCheckThrowsNoSuchFileException() throws IOException {
        final Path testFile = tempDir.resolve("removed.log");
        Files.writeString(testFile, "temp");

        final BasicFileAttributes attrs = mock(BasicFileAttributes.class);
        when(attrs.fileKey()).thenReturn("inode-10");
        when(fileOps.readAttributes(testFile)).thenReturn(attrs);

        final FileIdentity knownIdentity = FileIdentity.from(testFile, fileOps, FINGERPRINT_BYTES);

        when(fileOps.size(testFile)).thenThrow(new NoSuchFileException(testFile.toString()));

        final RotationResult result = rotationDetector.checkRotation(testFile, knownIdentity, 100L);

        assertThat(result.getRotationType(), equalTo(RotationType.DELETED));
    }

    @Test
    void noRotationWhenSizeCheckThrowsGenericIOException() throws IOException {
        final Path testFile = tempDir.resolve("error.log");
        Files.writeString(testFile, "content");

        final BasicFileAttributes attrs = mock(BasicFileAttributes.class);
        when(attrs.fileKey()).thenReturn("inode-20");
        when(fileOps.readAttributes(testFile)).thenReturn(attrs);

        final FileIdentity knownIdentity = FileIdentity.from(testFile, fileOps, FINGERPRINT_BYTES);

        when(fileOps.size(testFile)).thenThrow(new IOException("disk error"));

        final RotationResult result = rotationDetector.checkRotation(testFile, knownIdentity, 100L);

        assertThat(result.getRotationType(), equalTo(RotationType.NO_ROTATION));
    }

    @Test
    void noRotationWhenReadAttributesThrowsGenericRuntimeException() throws IOException {
        final Path testFile = tempDir.resolve("runtime-err.log");

        when(fileOps.readAttributes(testFile)).thenThrow(new RuntimeException("unexpected"));

        final FileIdentity knownIdentity = mock(FileIdentity.class);

        final RotationResult result = rotationDetector.checkRotation(testFile, knownIdentity, 100L);

        assertThat(result.getRotationType(), equalTo(RotationType.NO_ROTATION));
    }

    @Test
    void deletedFileWhenWrappedNoSuchFileExceptionInCause() throws IOException {
        final Path testFile = tempDir.resolve("wrapped.log");
        when(fileOps.exists(testFile)).thenReturn(false);

        final FileIdentity knownIdentity = mock(FileIdentity.class);

        final RotationResult result = rotationDetector.checkRotation(testFile, knownIdentity, 100L);

        assertThat(result.getRotationType(), equalTo(RotationType.DELETED));
    }

    @Test
    void noRotationWhenSizeEqualsCurrentOffset() throws IOException {
        final Path testFile = tempDir.resolve("exact.log");
        Files.writeString(testFile, "data");

        final BasicFileAttributes attrs = mock(BasicFileAttributes.class);
        when(attrs.fileKey()).thenReturn("inode-30");
        when(fileOps.readAttributes(testFile)).thenReturn(attrs);

        final FileIdentity knownIdentity = FileIdentity.from(testFile, fileOps, FINGERPRINT_BYTES);

        when(fileOps.size(testFile)).thenReturn(100L);

        final RotationResult result = rotationDetector.checkRotation(testFile, knownIdentity, 100L);

        assertThat(result.getRotationType(), equalTo(RotationType.NO_ROTATION));
    }

    @Test
    void deletedFileWhenDeeplyNestedNoSuchFileException() throws IOException {
        final Path testFile = tempDir.resolve("deep-nested.log");
        when(fileOps.exists(testFile)).thenReturn(false);

        final FileIdentity knownIdentity = mock(FileIdentity.class);

        final RotationResult result = rotationDetector.checkRotation(testFile, knownIdentity, 100L);

        assertThat(result.getRotationType(), equalTo(RotationType.DELETED));
    }

    @Test
    void isCausedByNoSuchFile_returns_true_when_exception_itself_is_NoSuchFileException() {
        final NoSuchFileException noSuchFile = new NoSuchFileException("test.log");

        assertThat(rotationDetector.isCausedByNoSuchFile(noSuchFile), equalTo(true));
    }

    @Test
    void constructorThrowsIllegalArgumentExceptionWhenFingerprintBytesIsZero() {
        assertThrows(IllegalArgumentException.class,
                () -> new RotationDetector(fileOps, 0));
    }

    @Test
    void constructorThrowsIllegalArgumentExceptionWhenFingerprintBytesIsNegative() {
        assertThrows(IllegalArgumentException.class,
                () -> new RotationDetector(fileOps, -5));
    }

    @Test
    void checkRotationReturnsDeletedWhenFileDoesNotExist() {
        final Path testFile = tempDir.resolve("not-exists.log");
        when(fileOps.exists(testFile)).thenReturn(false);

        final FileIdentity knownIdentity = mock(FileIdentity.class);

        final RotationResult result = rotationDetector.checkRotation(testFile, knownIdentity, 100L);

        assertThat(result.getRotationType(), equalTo(RotationType.DELETED));
    }

    @Test
    void isCausedByNoSuchFile_returns_true_when_deeply_nested_in_cause_chain() {
        final NoSuchFileException noSuchFile = new NoSuchFileException("deep.log");
        final RuntimeException mid = new RuntimeException("mid", noSuchFile);
        final RuntimeException outer = new RuntimeException("outer", mid);

        assertThat(rotationDetector.isCausedByNoSuchFile(outer), equalTo(true));
    }

    @Test
    void isCausedByNoSuchFile_returns_false_when_no_NoSuchFileException_in_chain() {
        final IOException ioException = new IOException("generic");
        final RuntimeException outer = new RuntimeException("outer", ioException);

        assertThat(rotationDetector.isCausedByNoSuchFile(outer), equalTo(false));
    }

    @Test
    void checkRotationReturnsDeletedWhenRuntimeExceptionWrapsNoSuchFileExceptionDeeply() throws IOException {
        final Path testFile = tempDir.resolve("deep-cause.log");

        final NoSuchFileException noSuchFile = new NoSuchFileException(testFile.toString());
        final RuntimeException mid = new RuntimeException("mid", noSuchFile);
        final RuntimeException outer = new RuntimeException("outer", mid);
        when(fileOps.readAttributes(testFile)).thenThrow(outer);

        final FileIdentity knownIdentity = mock(FileIdentity.class);

        final RotationResult result = rotationDetector.checkRotation(testFile, knownIdentity, 100L);

        assertThat(result.getRotationType(), equalTo(RotationType.DELETED));
    }
}
