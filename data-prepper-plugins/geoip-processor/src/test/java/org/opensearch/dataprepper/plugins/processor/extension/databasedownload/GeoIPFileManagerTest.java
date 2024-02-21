/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension.databasedownload;

import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertTrue;

class GeoIPFileManagerTest {
    private final String outputFilePath = "./src/test/resources/geoip/test";

    @Test
    void createFolderIfNotExistTest() {
        final GeoIPFileManager geoIPFileManager = new GeoIPFileManager();
        geoIPFileManager.createDirectoryIfNotExist(outputFilePath);

        final File file = new File(outputFilePath);
        assertTrue(file.exists());
    }

    @Test
    void deleteDirectoryTest() {
        final GeoIPFileManager geoIPFileManager = new GeoIPFileManager();
        geoIPFileManager.createDirectoryIfNotExist(outputFilePath);

        final File file = new File(outputFilePath);
        assertTrue(file.isDirectory());
        geoIPFileManager.deleteDirectory(file);
    }

}