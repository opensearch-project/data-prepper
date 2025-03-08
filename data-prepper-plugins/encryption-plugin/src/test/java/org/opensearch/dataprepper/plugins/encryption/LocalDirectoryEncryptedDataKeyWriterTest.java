/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

@ExtendWith(MockitoExtension.class)
class LocalDirectoryEncryptedDataKeyWriterTest {
    private static final String TEST_PARENT_DIRECTORY = UUID.randomUUID().toString();
    private static final String TEST_CHILD_DIRECTORY = "test-key";
    private static final String TEST_ENCRYPTED_DATA_KEY_DIRECTORY = String.format(
            "%s/%s", TEST_PARENT_DIRECTORY, TEST_CHILD_DIRECTORY);
    private static final String TEST_ENCRYPTED_DATA_KEY_VALUE = UUID.randomUUID().toString();

    @Captor
    private ArgumentCaptor<Path> keyFilePathArgumentCaptor;

    @Test
    void testWriteEncryptedDataKeySuccess_when_encryption_key_directory_exists() {
        final LocalDirectoryEncryptedDataKeyWriter objectUnderTest = new LocalDirectoryEncryptedDataKeyWriter(
                TEST_ENCRYPTED_DATA_KEY_DIRECTORY);
        final Path mockPath = mock(Path.class);
        try (final MockedStatic<Files> filesMockedStatic = mockStatic(Files.class)) {
            filesMockedStatic.when(() -> Files.exists(
                    eq(Path.of(TEST_ENCRYPTED_DATA_KEY_DIRECTORY)))).thenReturn(true);
            filesMockedStatic.when(() -> Files.writeString(any(Path.class), eq(TEST_ENCRYPTED_DATA_KEY_VALUE),
                            eq(StandardOpenOption.CREATE), eq(StandardOpenOption.TRUNCATE_EXISTING)))
                    .thenReturn(mockPath);
            objectUnderTest.writeEncryptedDataKey(TEST_ENCRYPTED_DATA_KEY_VALUE);
            filesMockedStatic.verify(() -> Files.writeString(
                    keyFilePathArgumentCaptor.capture(),
                    eq(TEST_ENCRYPTED_DATA_KEY_VALUE),
                    eq(StandardOpenOption.CREATE),
                    eq(StandardOpenOption.TRUNCATE_EXISTING)));
            final Path keyFile = keyFilePathArgumentCaptor.getValue();
            assertThat(keyFile.toString(), startsWith(TEST_ENCRYPTED_DATA_KEY_DIRECTORY));
            assertThat(keyFile.toString(), endsWith(".key"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testWriteEncryptedDataKeySuccess_when_encryption_key_directory_does_not_exist() {
        final LocalDirectoryEncryptedDataKeyWriter objectUnderTest = new LocalDirectoryEncryptedDataKeyWriter(
                TEST_ENCRYPTED_DATA_KEY_DIRECTORY);
        final Path mockPath = mock(Path.class);
        try (final MockedStatic<Files> filesMockedStatic = mockStatic(Files.class)) {
            filesMockedStatic.when(() -> Files.exists(
                    eq(Path.of(TEST_ENCRYPTED_DATA_KEY_DIRECTORY)))).thenReturn(false);
            filesMockedStatic.when(() -> Files.createDirectories(
                    eq(Path.of(TEST_ENCRYPTED_DATA_KEY_DIRECTORY)))).thenReturn(mockPath);
            filesMockedStatic.when(() -> Files.writeString(any(Path.class), eq(TEST_ENCRYPTED_DATA_KEY_VALUE),
                            eq(StandardOpenOption.CREATE), eq(StandardOpenOption.TRUNCATE_EXISTING)))
                    .thenReturn(mockPath);
            objectUnderTest.writeEncryptedDataKey(TEST_ENCRYPTED_DATA_KEY_VALUE);
            filesMockedStatic.verify(() -> Files.writeString(
                    keyFilePathArgumentCaptor.capture(),
                    eq(TEST_ENCRYPTED_DATA_KEY_VALUE),
                    eq(StandardOpenOption.CREATE),
                    eq(StandardOpenOption.TRUNCATE_EXISTING)));
            final Path keyFile = keyFilePathArgumentCaptor.getValue();
            assertThat(keyFile.toString(), startsWith(TEST_ENCRYPTED_DATA_KEY_DIRECTORY));
            assertThat(keyFile.toString(), endsWith(".key"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testWriteEncryptedDataKeyThrowsIOException_when_Files_writeString_throws() {
        final LocalDirectoryEncryptedDataKeyWriter objectUnderTest = new LocalDirectoryEncryptedDataKeyWriter(
                TEST_ENCRYPTED_DATA_KEY_DIRECTORY);
        try (final MockedStatic<Files> filesMockedStatic = mockStatic(Files.class)) {
            filesMockedStatic.when(() -> Files.exists(
                    eq(Path.of(TEST_ENCRYPTED_DATA_KEY_DIRECTORY)))).thenReturn(true);
            filesMockedStatic.when(() -> Files.writeString(any(Path.class), eq(TEST_ENCRYPTED_DATA_KEY_VALUE),
                            eq(StandardOpenOption.CREATE), eq(StandardOpenOption.TRUNCATE_EXISTING)))
                    .thenThrow(IOException.class);
            assertThrows(IOException.class, () -> objectUnderTest.writeEncryptedDataKey(TEST_ENCRYPTED_DATA_KEY_VALUE));
        }
    }
}