/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension.databasedownload;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.processor.databaseenrich.GeoIP2DatabaseReader;
import org.opensearch.dataprepper.plugins.processor.databaseenrich.GeoIPDatabaseReader;
import org.opensearch.dataprepper.plugins.processor.databaseenrich.GeoLite2DatabaseReader;
import org.opensearch.dataprepper.plugins.processor.extension.MaxMindConfig;
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
    private static final String CDN_ENDPOINT = "https://devo.geoip.maps.opensearch.org/v1/mmdb/geolite2/manifest.json";

    @Mock
    private MaxMindConfig maxMindConfig;

    private GeoIPDatabaseManager createObjectUnderTest() {
        return new GeoIPDatabaseManager(maxMindConfig);
    }

    @Test
    void test_constructor_with_geolite2_file_path_should_use_local_download_service_and_geolite2_reader() throws Exception {
        try (final MockedConstruction<LicenseTypeCheck> licenseTypeCheckMockedConstruction = mockConstruction(LicenseTypeCheck.class,(mock,context)->
                    when(mock.isGeoLite2OrEnterpriseLicense(any())).thenReturn(LicenseTypeOptions.FREE));
             final MockedConstruction<LocalDBDownloadService> localDBDownloadServiceMockedConstruction = mockConstruction(LocalDBDownloadService.class,
                     (mock2, context2)-> doNothing().when(mock2).initiateDownload(List.of(PATH)));
             final MockedConstruction<GeoLite2DatabaseReader> geoLite2DatabaseReaderMockedConstruction = mockConstruction(GeoLite2DatabaseReader.class)
        ) {

            when(maxMindConfig.getDatabasePaths()).thenReturn(List.of(PATH));
            createObjectUnderTest();

            assertThat(licenseTypeCheckMockedConstruction.constructed().size(), equalTo(1));
            verify(licenseTypeCheckMockedConstruction.constructed().get(0)).isGeoLite2OrEnterpriseLicense(any());
            assertThat(localDBDownloadServiceMockedConstruction.constructed().size(), equalTo(1));
            verify(localDBDownloadServiceMockedConstruction.constructed().get(0)).initiateDownload(List.of(PATH));
            assertThat(geoLite2DatabaseReaderMockedConstruction.constructed().size(), equalTo(1));
        }
    }

    @Test
    void test_constructor_with_geoip2_file_path_should_use_local_download_service_and_geoip2_reader() throws Exception {
        try (final MockedConstruction<LicenseTypeCheck> licenseTypeCheckMockedConstruction = mockConstruction(LicenseTypeCheck.class,(mock,context)->
                when(mock.isGeoLite2OrEnterpriseLicense(any())).thenReturn(LicenseTypeOptions.ENTERPRISE));
             final MockedConstruction<LocalDBDownloadService> localDBDownloadServiceMockedConstruction = mockConstruction(LocalDBDownloadService.class,
                     (mock2, context2)-> doNothing().when(mock2).initiateDownload(List.of(PATH)));
             final MockedConstruction<GeoIP2DatabaseReader> geoIP2DatabaseReaderMockedConstruction = mockConstruction(GeoIP2DatabaseReader.class)
        ) {

            when(maxMindConfig.getDatabasePaths()).thenReturn(List.of(PATH));
            createObjectUnderTest();

            assertThat(licenseTypeCheckMockedConstruction.constructed().size(), equalTo(1));
            verify(licenseTypeCheckMockedConstruction.constructed().get(0)).isGeoLite2OrEnterpriseLicense(any());
            assertThat(localDBDownloadServiceMockedConstruction.constructed().size(), equalTo(1));
            verify(localDBDownloadServiceMockedConstruction.constructed().get(0)).initiateDownload(List.of(PATH));
            assertThat(geoIP2DatabaseReaderMockedConstruction.constructed().size(), equalTo(1));
        }
    }

    @Test
    void test_constructor_with_geolite2_s3_uri_should_use_s3_download_service_and_geolite2_reader() {
        try (final MockedConstruction<LicenseTypeCheck> licenseTypeCheckMockedConstruction = mockConstruction(LicenseTypeCheck.class,(mock,context)->
                when(mock.isGeoLite2OrEnterpriseLicense(any())).thenReturn(LicenseTypeOptions.FREE));
             final MockedConstruction<S3DBService> s3DBServiceMockedConstruction = mockConstruction(S3DBService.class,
                     (mock2, context2)-> doNothing().when(mock2).initiateDownload(List.of(S3_URI)));
             final MockedConstruction<GeoLite2DatabaseReader> geoLite2DatabaseReaderMockedConstruction = mockConstruction(GeoLite2DatabaseReader.class)
        ) {

            when(maxMindConfig.getDatabasePaths()).thenReturn(List.of(S3_URI));
            createObjectUnderTest();

            assertThat(licenseTypeCheckMockedConstruction.constructed().size(), equalTo(1));
            verify(licenseTypeCheckMockedConstruction.constructed().get(0)).isGeoLite2OrEnterpriseLicense(any());
            assertThat(s3DBServiceMockedConstruction.constructed().size(), equalTo(1));
            verify(s3DBServiceMockedConstruction.constructed().get(0)).initiateDownload(List.of(S3_URI));
            assertThat(geoLite2DatabaseReaderMockedConstruction.constructed().size(), equalTo(1));
        }
    }

    @Test
    void test_constructor_with_geoip2_s3_uri_should_use_s3_download_service_and_geoip2_reader() {
        try (final MockedConstruction<LicenseTypeCheck> licenseTypeCheckMockedConstruction = mockConstruction(LicenseTypeCheck.class,(mock,context)->
                when(mock.isGeoLite2OrEnterpriseLicense(any())).thenReturn(LicenseTypeOptions.ENTERPRISE));
             final MockedConstruction<S3DBService> s3DBServiceMockedConstruction = mockConstruction(S3DBService.class,
                     (mock2, context2)-> doNothing().when(mock2).initiateDownload(List.of(S3_URI)));
             final MockedConstruction<GeoIP2DatabaseReader> geoIP2DatabaseReaderMockedConstruction = mockConstruction(GeoIP2DatabaseReader.class)
        ) {

            when(maxMindConfig.getDatabasePaths()).thenReturn(List.of(S3_URI));
            createObjectUnderTest();

            assertThat(licenseTypeCheckMockedConstruction.constructed().size(), equalTo(1));
            verify(licenseTypeCheckMockedConstruction.constructed().get(0)).isGeoLite2OrEnterpriseLicense(any());
            assertThat(s3DBServiceMockedConstruction.constructed().size(), equalTo(1));
            verify(s3DBServiceMockedConstruction.constructed().get(0)).initiateDownload(List.of(S3_URI));
            assertThat(geoIP2DatabaseReaderMockedConstruction.constructed().size(), equalTo(1));
        }
    }

    @Test
    void test_constructor_with_geolite2_url_should_use_http_download_service_and_geolite2_reader() {
        try (final MockedConstruction<LicenseTypeCheck> licenseTypeCheckMockedConstruction = mockConstruction(LicenseTypeCheck.class,(mock,context)->
                when(mock.isGeoLite2OrEnterpriseLicense(any())).thenReturn(LicenseTypeOptions.FREE));
             final MockedConstruction<HttpDBDownloadService> httpDBDownloadServiceMockedConstruction = mockConstruction(HttpDBDownloadService.class,
                     (mock2, context2)-> doNothing().when(mock2).initiateDownload(List.of(HTTPS_URL)));
            final MockedConstruction<GeoLite2DatabaseReader> geoLite2DatabaseReaderMockedConstruction = mockConstruction(GeoLite2DatabaseReader.class)
        ) {

            when(maxMindConfig.getDatabasePaths()).thenReturn(List.of(HTTPS_URL));
            createObjectUnderTest();

            assertThat(licenseTypeCheckMockedConstruction.constructed().size(), equalTo(1));
            verify(licenseTypeCheckMockedConstruction.constructed().get(0)).isGeoLite2OrEnterpriseLicense(any());
            assertThat(httpDBDownloadServiceMockedConstruction.constructed().size(), equalTo(1));
            verify(httpDBDownloadServiceMockedConstruction.constructed().get(0)).initiateDownload(List.of(HTTPS_URL));
            assertThat(geoLite2DatabaseReaderMockedConstruction.constructed().size(), equalTo(1));
        }
    }

    @Test
    void test_constructor_with_geoip2_url_should_use_http_download_service_and_geoip2_reader() {
        try (final MockedConstruction<LicenseTypeCheck> licenseTypeCheckMockedConstruction = mockConstruction(LicenseTypeCheck.class,(mock,context)->
                when(mock.isGeoLite2OrEnterpriseLicense(any())).thenReturn(LicenseTypeOptions.ENTERPRISE));
             final MockedConstruction<HttpDBDownloadService> httpDBDownloadServiceMockedConstruction = mockConstruction(HttpDBDownloadService.class,
                     (mock2, context2)-> doNothing().when(mock2).initiateDownload(List.of(HTTPS_URL)));
             final MockedConstruction<GeoIP2DatabaseReader> geoIP2DatabaseReaderMockedConstruction = mockConstruction(GeoIP2DatabaseReader.class)
        ) {

            when(maxMindConfig.getDatabasePaths()).thenReturn(List.of(HTTPS_URL));
            createObjectUnderTest();

            assertThat(licenseTypeCheckMockedConstruction.constructed().size(), equalTo(1));
            verify(licenseTypeCheckMockedConstruction.constructed().get(0)).isGeoLite2OrEnterpriseLicense(any());
            assertThat(httpDBDownloadServiceMockedConstruction.constructed().size(), equalTo(1));
            verify(httpDBDownloadServiceMockedConstruction.constructed().get(0)).initiateDownload(List.of(HTTPS_URL));
            assertThat(geoIP2DatabaseReaderMockedConstruction.constructed().size(), equalTo(1));
        }
    }

    @Test
    void test_constructor_with_geolite2_cdn_should_use_cdn_download_service_and_geolite2_reader() {
        try (final MockedConstruction<LicenseTypeCheck> licenseTypeCheckMockedConstruction = mockConstruction(LicenseTypeCheck.class,(mock,context)->
                when(mock.isGeoLite2OrEnterpriseLicense(any())).thenReturn(LicenseTypeOptions.FREE));
             final MockedConstruction<CDNDownloadService> cdnDownloadServiceMockedConstruction = mockConstruction(CDNDownloadService.class,
                     (mock2, context2)-> doNothing().when(mock2).initiateDownload(List.of(CDN_ENDPOINT)));
             final MockedConstruction<GeoLite2DatabaseReader> geoLite2DatabaseReaderMockedConstruction = mockConstruction(GeoLite2DatabaseReader.class)
        ) {

            when(maxMindConfig.getDatabasePaths()).thenReturn(List.of(CDN_ENDPOINT));
            final GeoIPDatabaseManager objectUnderTest = createObjectUnderTest();

            assertThat(licenseTypeCheckMockedConstruction.constructed().size(), equalTo(1));
            verify(licenseTypeCheckMockedConstruction.constructed().get(0)).isGeoLite2OrEnterpriseLicense(any());
            assertThat(cdnDownloadServiceMockedConstruction.constructed().size(), equalTo(1));
            verify(cdnDownloadServiceMockedConstruction.constructed().get(0)).initiateDownload(List.of(CDN_ENDPOINT));
            assertThat(geoLite2DatabaseReaderMockedConstruction.constructed().size(), equalTo(1));

            objectUnderTest.updateDatabaseReader();


        }
    }

    @Test
    void test_updateDatabaseReader_with_geolite2_cdn_should_use_cdn_download_service_and_geolite2_reader() {
        try (final MockedConstruction<LicenseTypeCheck> licenseTypeCheckMockedConstruction = mockConstruction(LicenseTypeCheck.class,(mock,context)->
                when(mock.isGeoLite2OrEnterpriseLicense(any())).thenReturn(LicenseTypeOptions.FREE));
             final MockedConstruction<CDNDownloadService> cdnDownloadServiceMockedConstruction = mockConstruction(CDNDownloadService.class,
                     (mock2, context2)-> doNothing().when(mock2).initiateDownload(List.of(CDN_ENDPOINT)));
             final MockedConstruction<GeoLite2DatabaseReader> geoLite2DatabaseReaderMockedConstruction = mockConstruction(GeoLite2DatabaseReader.class)
        ) {

            when(maxMindConfig.getDatabasePaths()).thenReturn(List.of(CDN_ENDPOINT));
            final GeoIPDatabaseManager objectUnderTest = createObjectUnderTest();

            assertThat(licenseTypeCheckMockedConstruction.constructed().size(), equalTo(1));
            verify(licenseTypeCheckMockedConstruction.constructed().get(0)).isGeoLite2OrEnterpriseLicense(any());
            assertThat(cdnDownloadServiceMockedConstruction.constructed().size(), equalTo(1));
            verify(cdnDownloadServiceMockedConstruction.constructed().get(0)).initiateDownload(List.of(CDN_ENDPOINT));
            assertThat(geoLite2DatabaseReaderMockedConstruction.constructed().size(), equalTo(1));


            objectUnderTest.updateDatabaseReader();
            assertThat(cdnDownloadServiceMockedConstruction.constructed().size(), equalTo(2));
            verify(cdnDownloadServiceMockedConstruction.constructed().get(0)).initiateDownload(List.of(CDN_ENDPOINT));
            assertThat(geoLite2DatabaseReaderMockedConstruction.constructed().size(), equalTo(2));
        }
    }

    @Test
    void test_getGeoIPDatabaseReader_with_geolite2_cdn_should_use_cdn_download_service_and_geolite2_reader() {
        try (final MockedConstruction<LicenseTypeCheck> licenseTypeCheckMockedConstruction = mockConstruction(LicenseTypeCheck.class,(mock,context)->
                when(mock.isGeoLite2OrEnterpriseLicense(any())).thenReturn(LicenseTypeOptions.FREE));
             final MockedConstruction<CDNDownloadService> cdnDownloadServiceMockedConstruction = mockConstruction(CDNDownloadService.class,
                     (mock2, context2)-> doNothing().when(mock2).initiateDownload(List.of(CDN_ENDPOINT)));
             final MockedConstruction<GeoLite2DatabaseReader> geoLite2DatabaseReaderMockedConstruction = mockConstruction(GeoLite2DatabaseReader.class)
        ) {

            when(maxMindConfig.getDatabasePaths()).thenReturn(List.of(CDN_ENDPOINT));
            final GeoIPDatabaseManager objectUnderTest = createObjectUnderTest();

            assertThat(licenseTypeCheckMockedConstruction.constructed().size(), equalTo(1));
            verify(licenseTypeCheckMockedConstruction.constructed().get(0)).isGeoLite2OrEnterpriseLicense(any());
            assertThat(cdnDownloadServiceMockedConstruction.constructed().size(), equalTo(1));
            verify(cdnDownloadServiceMockedConstruction.constructed().get(0)).initiateDownload(List.of(CDN_ENDPOINT));
            assertThat(geoLite2DatabaseReaderMockedConstruction.constructed().size(), equalTo(1));

            final GeoIPDatabaseReader geoIPDatabaseReader = objectUnderTest.getGeoIPDatabaseReader();
            assertThat(geoIPDatabaseReader, equalTo(geoLite2DatabaseReaderMockedConstruction.constructed().get(0)));
        }
    }
}
