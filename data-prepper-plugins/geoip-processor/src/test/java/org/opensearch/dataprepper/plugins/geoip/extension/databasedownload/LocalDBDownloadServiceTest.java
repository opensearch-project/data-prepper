/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.extension.databasedownload;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.geoip.extension.MaxMindDatabaseConfig;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LocalDBDownloadServiceTest {
    private final String destinationDirectory = "./src/test/resources/dest/";
    private final String sourceDirectory = "./src/test/resources/src/";
    private LocalDBDownloadService downloadThroughLocalPath;
    @Mock
    private MaxMindDatabaseConfig maxMindDatabaseConfig;

    @Test
    void initiateDownloadTest() throws IOException {
        downloadThroughLocalPath = createObjectUnderTest();
        generateSampleFiles();
        assertDoesNotThrow(() -> {
            downloadThroughLocalPath.initiateDownload();
        });

        assertTrue(new File(destinationDirectory + File.separator + "filename.mmdb").exists());
    }

    @Test
    void testOverwriteFunctionality() throws Exception {
        downloadThroughLocalPath = createObjectUnderTest();
        
        String initialContent = "Initial database content\nVersion: 1.0";
        createFileWithContent(sourceDirectory + File.separator + "SampleFile.mmdb", initialContent);
        
        downloadThroughLocalPath.initiateDownload();
        
        File destinationFile = new File(destinationDirectory + File.separator + "filename.mmdb");
        assertTrue(destinationFile.exists());
        
        String copiedContent = Files.readString(destinationFile.toPath());
        assertEquals(initialContent, copiedContent);
        
        String updatedContent = "Updated database content\nVersion: 2.0";
        createFileWithContent(sourceDirectory + File.separator + "SampleFile.mmdb", updatedContent);
        
        downloadThroughLocalPath.initiateDownload();
        
        String finalContent = Files.readString(destinationFile.toPath());
        assertEquals(updatedContent, finalContent);
    }

    private LocalDBDownloadService createObjectUnderTest() {
        when(maxMindDatabaseConfig.getDatabasePaths()).thenReturn(Map.of("filename", sourceDirectory + File.separator + "SampleFile.mmdb"));
        createFolder(destinationDirectory);
        return new LocalDBDownloadService(destinationDirectory, maxMindDatabaseConfig);
    }

    private void createFolder(String folderName) {
        File folder = new File(folderName);
        if (!folder.exists()) {
            folder.mkdir();
        }
    }

    private void generateSampleFiles() throws IOException {
        String fileName = "SampleFile.mmdb";
        String content = "This is sample file";

        createFolder(sourceDirectory);
        createFileWithContent(sourceDirectory + File.separator + fileName, content);
    }

    private void createFileWithContent(String filePath, String content) throws IOException {
        createFolder(sourceDirectory);
        try (FileWriter writer = new FileWriter(filePath)) {
            writer.write(content);
        }
    }

    @AfterEach
    void cleanUp() {
        deleteDirectory(new File(sourceDirectory));
        deleteDirectory(new File(destinationDirectory));
    }

    public void deleteDirectory(final File file) {
        if (file.exists()) {
            for (final File subFile : file.listFiles()) {
                if (subFile.isDirectory()) {
                    deleteDirectory(subFile);
                }
                subFile.delete();
            }
            file.delete();
        }
    }
}
