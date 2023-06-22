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

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

@ExtendWith(MockitoExtension.class)
class HttpDBDownloadServiceTest {

    private static final String PREFIX_DIR = "first_database";
    private HttpDBDownloadService downloadThroughUrl;

    @Test
    void initiateDownloadTest() throws NoSuchFieldException, IllegalAccessException {
        DatabasePathURLConfig databasePathURLConfig1 = new DatabasePathURLConfig();
        ReflectivelySetField.setField(DatabasePathURLConfig.class,
                databasePathURLConfig1, "url", "https://download.maxmind.com/app/geoip_download?" +
                        "edition_id=GeoLite2-ASN&license_key=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&suffix=tar.gz");
        List<DatabasePathURLConfig> config = new ArrayList<>();
        config.add(databasePathURLConfig1);
        downloadThroughUrl = createObjectUnderTest();
        assertDoesNotThrow(() -> {
            downloadThroughUrl.initiateDownload(config);
        });
    }

    private HttpDBDownloadService createObjectUnderTest() {
        return new HttpDBDownloadService(PREFIX_DIR);
    }
}