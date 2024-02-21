/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension.databasedownload;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.processor.exception.DownloadFailedException;

import java.io.File;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ManifestDownloadServiceTest {
    private static final String OUTPUT_DIR = "./src/test/resources/geoip";

    private ManifestDownloadService createObjectUnderTest() {
        return new ManifestDownloadService(OUTPUT_DIR);
    }

    @Test
    void test_with_valid_endpoint_should_download_file() {
        final ManifestDownloadService objectUnderTest = createObjectUnderTest();
        objectUnderTest.initiateDownload(List.of("https://devo.geoip.maps.opensearch.org/v1/mmdb/geolite2-city/manifest.json"));

        final File file = new File(OUTPUT_DIR + File.separator + "geolite2-city.mmdb");
        assertTrue(file.exists());

        file.deleteOnExit();
        final File directory = new File(OUTPUT_DIR);
        directory.deleteOnExit();
    }

    @Test
    void test_with_invalid_endpoint_should_throw_exception() {
        final ManifestDownloadService objectUnderTest = createObjectUnderTest();
        assertThrows(DownloadFailedException.class, () -> objectUnderTest
                .initiateDownload(List.of("https://devo.geoip.maps.opensearch.org/v1/mmdb/geolite2-enterprise/manifest.json")));
    }

}