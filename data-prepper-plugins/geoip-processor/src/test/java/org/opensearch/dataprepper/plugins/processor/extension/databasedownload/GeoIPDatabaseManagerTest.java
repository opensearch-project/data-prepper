/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension.databasedownload;

import org.junit.jupiter.api.BeforeEach;
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
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GeoIPDatabaseManagerTest {
    private static final String S3_URI = "s3://geoip/data";
    private static final String HTTPS_URL = "https://download.maxmind.com/app/geoip_download?editionid=GeoLite2-ASN&suffix=tar.gz";
    private static final String PATH = "./build/resources/test/mmdb-files/geo-lite2";
    private static final String CDN_ENDPOINT = "https://devo.geoip.maps.opensearch.org/v1/mmdb/geolite2/manifest.json";

    @Mock
    private MaxMindConfig maxMindConfig;

    @Mock
    private LicenseTypeCheck licenseTypeCheck;

    @Mock
    private DatabaseReaderBuilder databaseReaderBuilder;

    @Mock
    private ReentrantReadWriteLock.WriteLock writeLock;

    @Mock
    private GeoIPFileManager databaseFileManager;

    @BeforeEach
    void setUp() {
        when(licenseTypeCheck.isGeoLite2OrEnterpriseLicense(any())).thenReturn(LicenseTypeOptions.FREE);
    }

    private GeoIPDatabaseManager createObjectUnderTest() {
        return new GeoIPDatabaseManager(maxMindConfig, licenseTypeCheck, databaseReaderBuilder, databaseFileManager, writeLock);
    }

    @Test
    void test_initiateDatabaseDownload_with_geolite2_file_path_should_use_local_download_service_and_geolite2_reader() throws Exception {
        try (final MockedConstruction<LocalDBDownloadService> localDBDownloadServiceMockedConstruction = mockConstruction(LocalDBDownloadService.class,
                     (mock2, context2)-> doNothing().when(mock2).initiateDownload(List.of(PATH)));
             final MockedConstruction<GeoLite2DatabaseReader> geoLite2DatabaseReaderMockedConstruction = mockConstruction(GeoLite2DatabaseReader.class)
        ) {

            when(maxMindConfig.getDatabasePaths()).thenReturn(List.of(PATH));
            final GeoIPDatabaseManager objectUnderTest = createObjectUnderTest();
            objectUnderTest.initiateDatabaseDownload();

            assertThat(localDBDownloadServiceMockedConstruction.constructed().size(), equalTo(1));
            verify(localDBDownloadServiceMockedConstruction.constructed().get(0)).initiateDownload(List.of(PATH));
            assertThat(geoLite2DatabaseReaderMockedConstruction.constructed().size(), equalTo(1));

            final GeoIPDatabaseReader geoIPDatabaseReader = objectUnderTest.getGeoIPDatabaseReader();
            assertThat(geoIPDatabaseReader, equalTo(geoLite2DatabaseReaderMockedConstruction.constructed().get(0)));
        }
    }

    @Test
    void test_initiateDatabaseDownload_with_geoip2_file_path_should_use_local_download_service_and_geoip2_reader() throws Exception {
        try (final MockedConstruction<LocalDBDownloadService> localDBDownloadServiceMockedConstruction = mockConstruction(LocalDBDownloadService.class,
                     (mock2, context2)-> doNothing().when(mock2).initiateDownload(List.of(PATH)));
             final MockedConstruction<GeoIP2DatabaseReader> geoIP2DatabaseReaderMockedConstruction = mockConstruction(GeoIP2DatabaseReader.class)
        ) {
            when(licenseTypeCheck.isGeoLite2OrEnterpriseLicense(any())).thenReturn(LicenseTypeOptions.ENTERPRISE);
            when(maxMindConfig.getDatabasePaths()).thenReturn(List.of(PATH));

            final GeoIPDatabaseManager objectUnderTest = createObjectUnderTest();
            objectUnderTest.initiateDatabaseDownload();

            assertThat(localDBDownloadServiceMockedConstruction.constructed().size(), equalTo(1));
            verify(localDBDownloadServiceMockedConstruction.constructed().get(0)).initiateDownload(List.of(PATH));
            assertThat(geoIP2DatabaseReaderMockedConstruction.constructed().size(), equalTo(1));

            final GeoIPDatabaseReader geoIPDatabaseReader = objectUnderTest.getGeoIPDatabaseReader();
            assertThat(geoIPDatabaseReader, equalTo(geoIP2DatabaseReaderMockedConstruction.constructed().get(0)));
        }
    }

    @Test
    void test_initiateDatabaseDownload_with_geolite2_s3_uri_should_use_s3_download_service_and_geolite2_reader() {
        try (final MockedConstruction<S3DBService> s3DBServiceMockedConstruction = mockConstruction(S3DBService.class,
                     (mock2, context2)-> doNothing().when(mock2).initiateDownload(List.of(S3_URI)));
             final MockedConstruction<GeoLite2DatabaseReader> geoLite2DatabaseReaderMockedConstruction = mockConstruction(GeoLite2DatabaseReader.class)
        ) {

            when(maxMindConfig.getDatabasePaths()).thenReturn(List.of(S3_URI));
            final GeoIPDatabaseManager objectUnderTest = createObjectUnderTest();
            objectUnderTest.initiateDatabaseDownload();

            assertThat(s3DBServiceMockedConstruction.constructed().size(), equalTo(1));
            verify(s3DBServiceMockedConstruction.constructed().get(0)).initiateDownload(List.of(S3_URI));
            assertThat(geoLite2DatabaseReaderMockedConstruction.constructed().size(), equalTo(1));

            final GeoIPDatabaseReader geoIPDatabaseReader = objectUnderTest.getGeoIPDatabaseReader();
            assertThat(geoIPDatabaseReader, equalTo(geoLite2DatabaseReaderMockedConstruction.constructed().get(0)));
        }
    }

    @Test
    void test_initiateDatabaseDownload_with_geoip2_s3_uri_should_use_s3_download_service_and_geoip2_reader() {
        try (final MockedConstruction<S3DBService> s3DBServiceMockedConstruction = mockConstruction(S3DBService.class,
                     (mock2, context2)-> doNothing().when(mock2).initiateDownload(List.of(S3_URI)));
             final MockedConstruction<GeoIP2DatabaseReader> geoIP2DatabaseReaderMockedConstruction = mockConstruction(GeoIP2DatabaseReader.class)
        ) {
            when(licenseTypeCheck.isGeoLite2OrEnterpriseLicense(any())).thenReturn(LicenseTypeOptions.ENTERPRISE);
            when(maxMindConfig.getDatabasePaths()).thenReturn(List.of(S3_URI));

            final GeoIPDatabaseManager objectUnderTest = createObjectUnderTest();
            objectUnderTest.initiateDatabaseDownload();

            assertThat(s3DBServiceMockedConstruction.constructed().size(), equalTo(1));
            verify(s3DBServiceMockedConstruction.constructed().get(0)).initiateDownload(List.of(S3_URI));
            assertThat(geoIP2DatabaseReaderMockedConstruction.constructed().size(), equalTo(1));

            final GeoIPDatabaseReader geoIPDatabaseReader = objectUnderTest.getGeoIPDatabaseReader();
            assertThat(geoIPDatabaseReader, equalTo(geoIP2DatabaseReaderMockedConstruction.constructed().get(0)));
        }
    }

    @Test
    void test_initiateDatabaseDownload_with_geolite2_url_should_use_http_download_service_and_geolite2_reader() {
        try (final MockedConstruction<HttpDBDownloadService> httpDBDownloadServiceMockedConstruction = mockConstruction(HttpDBDownloadService.class,
                     (mock2, context2)-> doNothing().when(mock2).initiateDownload(List.of(HTTPS_URL)));
            final MockedConstruction<GeoLite2DatabaseReader> geoLite2DatabaseReaderMockedConstruction = mockConstruction(GeoLite2DatabaseReader.class)
        ) {

            when(maxMindConfig.getDatabasePaths()).thenReturn(List.of(HTTPS_URL));
            final GeoIPDatabaseManager objectUnderTest = createObjectUnderTest();
            objectUnderTest.initiateDatabaseDownload();

            assertThat(httpDBDownloadServiceMockedConstruction.constructed().size(), equalTo(1));
            verify(httpDBDownloadServiceMockedConstruction.constructed().get(0)).initiateDownload(List.of(HTTPS_URL));
            assertThat(geoLite2DatabaseReaderMockedConstruction.constructed().size(), equalTo(1));

            final GeoIPDatabaseReader geoIPDatabaseReader = objectUnderTest.getGeoIPDatabaseReader();
            assertThat(geoIPDatabaseReader, equalTo(geoLite2DatabaseReaderMockedConstruction.constructed().get(0)));
        }
    }

    @Test
    void test_initiateDatabaseDownload_with_geoip2_url_should_use_http_download_service_and_geoip2_reader() {
        try (final MockedConstruction<HttpDBDownloadService> httpDBDownloadServiceMockedConstruction = mockConstruction(HttpDBDownloadService.class,
                     (mock2, context2)-> doNothing().when(mock2).initiateDownload(List.of(HTTPS_URL)));
             final MockedConstruction<GeoIP2DatabaseReader> geoIP2DatabaseReaderMockedConstruction = mockConstruction(GeoIP2DatabaseReader.class)
        ) {
            when(licenseTypeCheck.isGeoLite2OrEnterpriseLicense(any())).thenReturn(LicenseTypeOptions.ENTERPRISE);
            when(maxMindConfig.getDatabasePaths()).thenReturn(List.of(HTTPS_URL));

            final GeoIPDatabaseManager objectUnderTest = createObjectUnderTest();
            objectUnderTest.initiateDatabaseDownload();

            assertThat(httpDBDownloadServiceMockedConstruction.constructed().size(), equalTo(1));
            verify(httpDBDownloadServiceMockedConstruction.constructed().get(0)).initiateDownload(List.of(HTTPS_URL));
            assertThat(geoIP2DatabaseReaderMockedConstruction.constructed().size(), equalTo(1));

            final GeoIPDatabaseReader geoIPDatabaseReader = objectUnderTest.getGeoIPDatabaseReader();
            assertThat(geoIPDatabaseReader, equalTo(geoIP2DatabaseReaderMockedConstruction.constructed().get(0)));
        }
    }

    @Test
    void test_initiateDatabaseDownload_with_geolite2_cdn_should_use_cdn_download_service_and_geolite2_reader() {
        try (final MockedConstruction<CDNDownloadService> cdnDownloadServiceMockedConstruction = mockConstruction(CDNDownloadService.class,
                     (mock2, context2)-> doNothing().when(mock2).initiateDownload(List.of(CDN_ENDPOINT)));
             final MockedConstruction<GeoLite2DatabaseReader> geoLite2DatabaseReaderMockedConstruction = mockConstruction(GeoLite2DatabaseReader.class)
        ) {

            when(maxMindConfig.getDatabasePaths()).thenReturn(List.of(CDN_ENDPOINT));
            final GeoIPDatabaseManager objectUnderTest = createObjectUnderTest();
            objectUnderTest.initiateDatabaseDownload();

            assertThat(cdnDownloadServiceMockedConstruction.constructed().size(), equalTo(1));
            verify(cdnDownloadServiceMockedConstruction.constructed().get(0)).initiateDownload(List.of(CDN_ENDPOINT));
            assertThat(geoLite2DatabaseReaderMockedConstruction.constructed().size(), equalTo(1));

            final GeoIPDatabaseReader geoIPDatabaseReader = objectUnderTest.getGeoIPDatabaseReader();
            assertThat(geoIPDatabaseReader, equalTo(geoLite2DatabaseReaderMockedConstruction.constructed().get(0)));
        }
    }

    @Test
    void test_updateDatabaseReader_with_geolite2_cdn_should_use_cdn_download_service_and_geolite2_reader_and_get_new_reader() {
        try (final MockedConstruction<CDNDownloadService> cdnDownloadServiceMockedConstruction = mockConstruction(CDNDownloadService.class,
                     (mock2, context2)-> doNothing().when(mock2).initiateDownload(List.of(CDN_ENDPOINT)));
             final MockedConstruction<GeoLite2DatabaseReader> geoLite2DatabaseReaderMockedConstruction = mockConstruction(GeoLite2DatabaseReader.class)
        ) {
            when(maxMindConfig.getDatabasePaths()).thenReturn(List.of(CDN_ENDPOINT));
            final GeoIPDatabaseManager objectUnderTest = createObjectUnderTest();

            objectUnderTest.initiateDatabaseDownload();
            final GeoIPDatabaseReader geoIPDatabaseReader = objectUnderTest.getGeoIPDatabaseReader();
            assertThat(geoIPDatabaseReader, equalTo(geoLite2DatabaseReaderMockedConstruction.constructed().get(0)));

            objectUnderTest.updateDatabaseReader();

            assertThat(cdnDownloadServiceMockedConstruction.constructed().size(), equalTo(2));
            for (CDNDownloadService cdnDownloadService: cdnDownloadServiceMockedConstruction.constructed()) {
                verify(cdnDownloadService).initiateDownload(List.of(CDN_ENDPOINT));
            }
            assertThat(geoLite2DatabaseReaderMockedConstruction.constructed().size(), equalTo(2));
            // verify if first instance is closed
            verify(geoLite2DatabaseReaderMockedConstruction.constructed().get(0)).close();
            final GeoIPDatabaseReader updatedGeoIPDatabaseReader = objectUnderTest.getGeoIPDatabaseReader();
            assertThat(updatedGeoIPDatabaseReader, equalTo(geoLite2DatabaseReaderMockedConstruction.constructed().get(1)));
        }
    }
}
