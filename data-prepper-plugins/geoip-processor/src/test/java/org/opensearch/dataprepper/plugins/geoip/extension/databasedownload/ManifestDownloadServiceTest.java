/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.extension.databasedownload;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.geoip.exception.DownloadFailedException;
import org.opensearch.dataprepper.plugins.geoip.extension.MaxMindDatabaseConfig;

import java.io.File;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ManifestDownloadServiceTest {
    private static final String OUTPUT_DIR = "./src/test/resources/geoip";
    private String path = "https://geoip.maps.opensearch.org/v1/mmdb/geolite2-city/manifest.json";
    private String invalid_path = "https://geoip.maps.opensearch.org/v1/mmdb/geolite2-test/manifest.json";

    @Mock
    private MaxMindDatabaseConfig maxMindDatabaseConfig;

    private ManifestDownloadService createObjectUnderTest() {
        return new ManifestDownloadService(OUTPUT_DIR, maxMindDatabaseConfig);
    }

    @Test
    void test_with_valid_endpoint_should_download_file() {
        when(maxMindDatabaseConfig.getDatabasePaths()).thenReturn(Map.of("geolite2-city", path));
        final ManifestDownloadService objectUnderTest = createObjectUnderTest();
        objectUnderTest.initiateDownload();

        final File file = new File(OUTPUT_DIR + File.separator + "geolite2-city.mmdb");
        assertTrue(file.exists());

        file.deleteOnExit();
        final File directory = new File(OUTPUT_DIR);
        directory.deleteOnExit();
    }

    @Test
    void test_with_invalid_endpoint_should_throw_exception() {
        when(maxMindDatabaseConfig.getDatabasePaths()).thenReturn(Map.of("geolite2-city", invalid_path));
        final ManifestDownloadService objectUnderTest = createObjectUnderTest();
        assertThrows(DownloadFailedException.class, () -> objectUnderTest.initiateDownload());
    }

}