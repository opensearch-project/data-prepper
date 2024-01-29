/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension.databasedownload;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

@ExtendWith(MockitoExtension.class)
class HttpDBDownloadServiceTest {

    private static final String PREFIX_DIR = "first_database";
    private HttpDBDownloadService downloadThroughUrl;

    @Test
    void initiateDownloadTest() {
        final String databasePath = "https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-ASN&suffix=tar.gz";

        downloadThroughUrl = createObjectUnderTest();
        assertDoesNotThrow(() -> {
            downloadThroughUrl.initiateDownload(List.of(databasePath));
        });
    }

    private HttpDBDownloadService createObjectUnderTest() {
        return new HttpDBDownloadService(PREFIX_DIR);
    }
}