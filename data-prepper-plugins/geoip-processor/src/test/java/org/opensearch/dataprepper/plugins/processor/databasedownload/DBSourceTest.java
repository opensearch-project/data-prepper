/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.databasedownload;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mockStatic;

@ExtendWith(MockitoExtension.class)
class DBSourceTest {

    private final String outputFilePath = System.getProperty("java.io.tmpdir") + "GeoTest";

    @Test
    void createFolderIfNotExistTest() {
        try (MockedStatic<DBSource> mockedStatic = mockStatic(DBSource.class)) {
            mockedStatic.when(() -> DBSource.createFolderIfNotExist(outputFilePath)).thenReturn(new File(outputFilePath));
            File actualFile = new File(outputFilePath);
            assertEquals(actualFile, DBSource.createFolderIfNotExist(outputFilePath));
        }
    }

    @Test
    void deleteDirectoryTest() {
        DBSource.createFolderIfNotExist(outputFilePath);
        DBSource.createFolderIfNotExist(outputFilePath + File.separator + "GeoIPz");
        DBSource.createFolderIfNotExist(outputFilePath + File.separator + "GeoIPx");
        assertDoesNotThrow(() -> {
            DBSource.deleteDirectory(new File(outputFilePath));
        });
    }
}