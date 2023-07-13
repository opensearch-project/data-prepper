/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.databasedownload;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.processor.configuration.DatabasePathURLConfig;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

@ExtendWith(MockitoExtension.class)
class LocalDBDownloadServiceTest {

    private static final String PREFIX_DIR = "geo-lite2";
    String tempFolderPath = System.getProperty("java.io.tmpdir") + File.separator + "GeoIP";
    String srcDir = System.getProperty("java.io.tmpdir") + File.separator + "Maxmind";
    private LocalDBDownloadService downloadThroughLocalPath;

    @Test
    void initiateDownloadTest() throws Exception {
        DatabasePathURLConfig databasePathURLConfig = new DatabasePathURLConfig();
        ReflectivelySetField.setField(DatabasePathURLConfig.class,
                databasePathURLConfig, "url", srcDir);
        createFolder(System.getProperty("java.io.tmpdir") + File.separator + "Maxmind");
        generateSampleFiles(srcDir, 5);
        List<DatabasePathURLConfig> config = new ArrayList<>();
        config.add(databasePathURLConfig);
        downloadThroughLocalPath = createObjectUnderTest();
        assertDoesNotThrow(() -> {
            downloadThroughLocalPath.initiateDownload(config);
        });
    }

    private LocalDBDownloadService createObjectUnderTest() {
        return new LocalDBDownloadService(tempFolderPath, PREFIX_DIR);
    }

    private static void createFolder(String folderName) {
        File folder = new File(folderName);
        if (!folder.exists()) {
            folder.mkdir();
        }
    }

    private static void generateSampleFiles(String folderName, int numFiles) {
        for (int i = 1; i <= numFiles; i++) {
            String fileName = "SampleFile" + i + ".txt";
            String content = "This is sample file " + i;

            try (FileWriter writer = new FileWriter(folderName + File.separator + fileName)) {
                writer.write(content);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}