/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.encryption;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;

class LocalDirectoryEncryptedDataKeySupplierTest {
    private static final String TEST_PARENT_DIRECTORY = UUID.randomUUID().toString();
    private static final String TEST_CHILD_DIRECTORY = "test-key";
    private static final String TEST_ENCRYPTED_DATA_KEY_DIRECTORY = String.format(
            "%s/%s", TEST_PARENT_DIRECTORY, TEST_CHILD_DIRECTORY);
    private static final String TEST_ENCRYPTED_DATA_KEY_VALUE = UUID.randomUUID().toString();
    private static final String TEST_FILE_1 = "test-file-1.key";
    private static final String TEST_FILE_2 = "test-file-2.key";
    private static final Path TEST_FILE_1_PATH = Paths.get(TEST_ENCRYPTED_DATA_KEY_DIRECTORY, TEST_FILE_1);
    private static final Path TEST_FILE_2_PATH = Paths.get(TEST_ENCRYPTED_DATA_KEY_DIRECTORY, TEST_FILE_2);

    @Test
    void testRetrieveValue() {
        try (final MockedStatic<Files> filesMockedStatic = mockStatic(Files.class)) {
            filesMockedStatic.when(() -> Files.list(any()))
                    .thenReturn(Stream.of(TEST_FILE_1_PATH, TEST_FILE_2_PATH));
            filesMockedStatic.when(() -> Files.readString(eq(TEST_FILE_2_PATH), eq(StandardCharsets.UTF_8)))
                    .thenReturn(TEST_ENCRYPTED_DATA_KEY_VALUE);
            final LocalDirectoryEncryptedDataKeySupplier objectUnderTest = new LocalDirectoryEncryptedDataKeySupplier(
                    TEST_ENCRYPTED_DATA_KEY_DIRECTORY);
            assertThat(objectUnderTest.retrieveValue(), equalTo(TEST_ENCRYPTED_DATA_KEY_VALUE));
        }
    }
}