/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension.database;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.processor.extension.MaxMindConfig;
import org.opensearch.dataprepper.plugins.processor.extension.databasedownload.HttpDBDownloadService;
import org.opensearch.dataprepper.plugins.processor.extension.databasedownload.LicenseTypeOptions;
import org.opensearch.dataprepper.plugins.processor.extension.databasedownload.LocalDBDownloadService;
import org.opensearch.dataprepper.plugins.processor.extension.databasedownload.S3DBService;
import org.opensearch.dataprepper.plugins.processor.utils.LicenseTypeCheck;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GeoIPDatabaseManagerTest {
    private static final String S3_URI = "https://mybucket10012023.s3.amazonaws.com/GeoLite2/";
    private static final String HTTPS_URL = "https://download.maxmind.com/app/geoip_download?editionid=GeoLite2-ASN&suffix=tar.gz";
    private static final String PATH = "./src/test/resources/mmdb-file/geo-lite2";

    @Mock
    private MaxMindConfig maxMindConfig;

    private GeoIPDatabaseManager createObjectUnderTest() {
        return new GeoIPDatabaseManager(maxMindConfig);
    }

    @Test
    void test_constructor_with_file_path_should_use_local_download_service() throws Exception {
        try (final MockedConstruction<LicenseTypeCheck> licenseTypeCheckMockedConstruction = mockConstruction(LicenseTypeCheck.class,(mock,context)->
                    when(mock.isGeoLite2OrEnterpriseLicense(any())).thenReturn(LicenseTypeOptions.FREE));
             final MockedConstruction<LocalDBDownloadService> localDBDownloadServiceMockedConstruction = mockConstruction(LocalDBDownloadService.class,
                     (mock2, context2)-> doNothing().when(mock2).initiateDownload(List.of(PATH)));
        ) {

            when(maxMindConfig.getDatabasePaths()).thenReturn(List.of(PATH));
            final GeoIPDatabaseManager objectUnderTest = createObjectUnderTest();

            verify(licenseTypeCheckMockedConstruction.constructed().get(0)).isGeoLite2OrEnterpriseLicense(any());
            assertThat(localDBDownloadServiceMockedConstruction.constructed().size(), equalTo(1));
            verify(localDBDownloadServiceMockedConstruction.constructed().get(0)).initiateDownload(List.of(PATH));
        }
    }

    @Test
    void test_constructor_with_s3_uri_should_use_s3_download_service() {
        try (final MockedConstruction<LicenseTypeCheck> licenseTypeCheckMockedConstruction = mockConstruction(LicenseTypeCheck.class,(mock,context)->
                when(mock.isGeoLite2OrEnterpriseLicense(any())).thenReturn(LicenseTypeOptions.FREE));
             final MockedConstruction<S3DBService> s3DBServiceMockedConstruction = mockConstruction(S3DBService.class)) {

            when(maxMindConfig.getDatabasePaths()).thenReturn(List.of(S3_URI));
            final GeoIPDatabaseManager objectUnderTest = createObjectUnderTest();

            verify(licenseTypeCheckMockedConstruction.constructed().get(0)).isGeoLite2OrEnterpriseLicense(any());
            assertThat(s3DBServiceMockedConstruction.constructed().size(), equalTo(1));
            verify(s3DBServiceMockedConstruction.constructed().get(0)).initiateDownload(List.of(S3_URI));
        }
    }

    @Test
    void test_constructor_with_url_should_use_http_download_service() {
        try (final MockedConstruction<LicenseTypeCheck> licenseTypeCheckMockedConstruction = mockConstruction(LicenseTypeCheck.class,(mock,context)->
                when(mock.isGeoLite2OrEnterpriseLicense(any())).thenReturn(LicenseTypeOptions.FREE));
             final MockedConstruction<HttpDBDownloadService> httpDBDownloadServiceMockedConstruction = mockConstruction(HttpDBDownloadService.class);) {

            when(maxMindConfig.getDatabasePaths()).thenReturn(List.of(HTTPS_URL));
            final GeoIPDatabaseManager objectUnderTest = createObjectUnderTest();

            verify(licenseTypeCheckMockedConstruction.constructed().get(0)).isGeoLite2OrEnterpriseLicense(any());
            assertThat(httpDBDownloadServiceMockedConstruction.constructed().size(), equalTo(1));
            verify(httpDBDownloadServiceMockedConstruction.constructed().get(0)).initiateDownload(List.of(HTTPS_URL));
        }
    }
}
