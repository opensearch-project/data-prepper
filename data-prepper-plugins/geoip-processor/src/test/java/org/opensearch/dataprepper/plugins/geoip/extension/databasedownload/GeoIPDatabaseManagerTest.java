/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.extension.databasedownload;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.geoip.databaseenrich.AutoCountingDatabaseReader;
import org.opensearch.dataprepper.plugins.geoip.databaseenrich.GeoIP2DatabaseReader;
import org.opensearch.dataprepper.plugins.geoip.databaseenrich.GeoIPDatabaseReader;
import org.opensearch.dataprepper.plugins.geoip.databaseenrich.GeoLite2DatabaseReader;
import org.opensearch.dataprepper.plugins.geoip.exception.DownloadFailedException;
import org.opensearch.dataprepper.plugins.geoip.extension.MaxMindConfig;
import org.opensearch.dataprepper.plugins.geoip.extension.MaxMindDatabaseConfig;
import org.opensearch.dataprepper.plugins.geoip.utils.LicenseTypeCheck;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GeoIPDatabaseManagerTest {
    private static final String S3_URI = "s3://geoip/data/GeoLite2-Country-Test.mmdb";
    private static final String HTTPS_URL = "https://download.maxmind.com/app/geoip_download?editionid=GeoLite2-ASN&suffix=tar.gz";
    private static final String PATH = "./build/resources/test/mmdb-files/geo-lite2/GeoLite2-Country-Test.mmdb";
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

    @Mock
    private MaxMindDatabaseConfig maxMindDatabaseConfig;

    @BeforeEach
    void setUp() {
        lenient().when(licenseTypeCheck.isGeoLite2OrEnterpriseLicense(any())).thenReturn(LicenseTypeOptions.FREE);
        lenient().when(maxMindConfig.getMaxMindDatabaseConfig()).thenReturn(maxMindDatabaseConfig);
    }

    private GeoIPDatabaseManager createObjectUnderTest() {
        return new GeoIPDatabaseManager(maxMindConfig, licenseTypeCheck, databaseReaderBuilder, databaseFileManager, writeLock);
    }

    @Test
    void test_initiateDatabaseDownload_with_geolite2_file_path_should_use_local_download_service_and_geolite2_reader() throws Exception {
        try (final MockedConstruction<LocalDBDownloadService> localDBDownloadServiceMockedConstruction = mockConstruction(LocalDBDownloadService.class,
                     (mock2, context2)-> doNothing().when(mock2).initiateDownload());
             final MockedConstruction<GeoLite2DatabaseReader> geoLite2DatabaseReaderMockedConstruction = mockConstruction(GeoLite2DatabaseReader.class);
             final MockedConstruction<AutoCountingDatabaseReader> autoCountingDatabaseReaderMockedConstruction = mockConstruction(AutoCountingDatabaseReader.class)
        ) {

            final HashMap<String, String> databases = new HashMap<>();
            databases.put("geolite2_country", PATH);
            when(maxMindDatabaseConfig.getDatabasePaths()).thenReturn(databases);
            final GeoIPDatabaseManager objectUnderTest = createObjectUnderTest();
            objectUnderTest.initiateDatabaseDownload();

            assertThat(localDBDownloadServiceMockedConstruction.constructed().size(), equalTo(1));
            verify(localDBDownloadServiceMockedConstruction.constructed().get(0)).initiateDownload();
            assertThat(geoLite2DatabaseReaderMockedConstruction.constructed().size(), equalTo(1));
            assertThat(autoCountingDatabaseReaderMockedConstruction.constructed().size(), equalTo(1));

            final GeoIPDatabaseReader geoIPDatabaseReader = objectUnderTest.getGeoIPDatabaseReader();
            assertThat(geoIPDatabaseReader, equalTo(autoCountingDatabaseReaderMockedConstruction.constructed().get(0)));
        }
    }

    @Test
    void test_initiateDatabaseDownload_with_geoip2_file_path_should_use_local_download_service_and_geoip2_reader() throws Exception {
        try (final MockedConstruction<LocalDBDownloadService> localDBDownloadServiceMockedConstruction = mockConstruction(LocalDBDownloadService.class,
                     (mock2, context2)-> doNothing().when(mock2).initiateDownload());
             final MockedConstruction<GeoIP2DatabaseReader> geoIP2DatabaseReaderMockedConstruction = mockConstruction(GeoIP2DatabaseReader.class);
             final MockedConstruction<AutoCountingDatabaseReader> autoCountingDatabaseReaderMockedConstruction = mockConstruction(AutoCountingDatabaseReader.class)
        ) {
            when(licenseTypeCheck.isGeoLite2OrEnterpriseLicense(any())).thenReturn(LicenseTypeOptions.ENTERPRISE);
            final HashMap<String, String> databases = new HashMap<>();
            databases.put("geolite2_country", PATH);
            when(maxMindDatabaseConfig.getDatabasePaths()).thenReturn(databases);

            final GeoIPDatabaseManager objectUnderTest = createObjectUnderTest();
            objectUnderTest.initiateDatabaseDownload();

            assertThat(localDBDownloadServiceMockedConstruction.constructed().size(), equalTo(1));
            verify(localDBDownloadServiceMockedConstruction.constructed().get(0)).initiateDownload();
            assertThat(geoIP2DatabaseReaderMockedConstruction.constructed().size(), equalTo(1));
            assertThat(autoCountingDatabaseReaderMockedConstruction.constructed().size(), equalTo(1));

            final GeoIPDatabaseReader geoIPDatabaseReader = objectUnderTest.getGeoIPDatabaseReader();
            assertThat(geoIPDatabaseReader, equalTo(autoCountingDatabaseReaderMockedConstruction.constructed().get(0)));
        }
    }

    @Test
    void test_initiateDatabaseDownload_with_geolite2_s3_uri_should_use_s3_download_service_and_geolite2_reader() {
        try (final MockedConstruction<S3DBService> s3DBServiceMockedConstruction = mockConstruction(S3DBService.class,
                     (mock2, context2)-> doNothing().when(mock2).initiateDownload());
             final MockedConstruction<GeoLite2DatabaseReader> geoLite2DatabaseReaderMockedConstruction = mockConstruction(GeoLite2DatabaseReader.class);
             final MockedConstruction<AutoCountingDatabaseReader> autoCountingDatabaseReaderMockedConstruction = mockConstruction(AutoCountingDatabaseReader.class)
        ) {

            final HashMap<String, String> databases = new HashMap<>();
            databases.put("geolite2_country", S3_URI);
            when(maxMindDatabaseConfig.getDatabasePaths()).thenReturn(databases);
            when(maxMindConfig.getMaxMindDatabaseConfig()).thenReturn(maxMindDatabaseConfig);
            final GeoIPDatabaseManager objectUnderTest = createObjectUnderTest();
            objectUnderTest.initiateDatabaseDownload();

            assertThat(s3DBServiceMockedConstruction.constructed().size(), equalTo(1));
            verify(s3DBServiceMockedConstruction.constructed().get(0)).initiateDownload();
            assertThat(geoLite2DatabaseReaderMockedConstruction.constructed().size(), equalTo(1));
            assertThat(autoCountingDatabaseReaderMockedConstruction.constructed().size(), equalTo(1));

            final GeoIPDatabaseReader geoIPDatabaseReader = objectUnderTest.getGeoIPDatabaseReader();
            assertThat(geoIPDatabaseReader, equalTo(autoCountingDatabaseReaderMockedConstruction.constructed().get(0)));
        }
    }

    @Test
    void test_initiateDatabaseDownload_with_geoip2_s3_uri_should_use_s3_download_service_and_geoip2_reader() {
        try (final MockedConstruction<S3DBService> s3DBServiceMockedConstruction = mockConstruction(S3DBService.class,
                     (mock2, context2)-> doNothing().when(mock2).initiateDownload());
             final MockedConstruction<GeoIP2DatabaseReader> geoIP2DatabaseReaderMockedConstruction = mockConstruction(GeoIP2DatabaseReader.class);
             final MockedConstruction<AutoCountingDatabaseReader> autoCountingDatabaseReaderMockedConstruction = mockConstruction(AutoCountingDatabaseReader.class)
        ) {
            when(licenseTypeCheck.isGeoLite2OrEnterpriseLicense(any())).thenReturn(LicenseTypeOptions.ENTERPRISE);
            final HashMap<String, String> databases = new HashMap<>();
            databases.put("geolite2_country", S3_URI);
            when(maxMindDatabaseConfig.getDatabasePaths()).thenReturn(databases);
            when(maxMindConfig.getMaxMindDatabaseConfig()).thenReturn(maxMindDatabaseConfig);

            final GeoIPDatabaseManager objectUnderTest = createObjectUnderTest();
            objectUnderTest.initiateDatabaseDownload();

            assertThat(s3DBServiceMockedConstruction.constructed().size(), equalTo(1));
            verify(s3DBServiceMockedConstruction.constructed().get(0)).initiateDownload();
            assertThat(geoIP2DatabaseReaderMockedConstruction.constructed().size(), equalTo(1));
            assertThat(autoCountingDatabaseReaderMockedConstruction.constructed().size(), equalTo(1));

            final GeoIPDatabaseReader geoIPDatabaseReader = objectUnderTest.getGeoIPDatabaseReader();
            assertThat(geoIPDatabaseReader, equalTo(autoCountingDatabaseReaderMockedConstruction.constructed().get(0)));
        }
    }

    @Test
    void test_initiateDatabaseDownload_with_geolite2_url_should_use_http_download_service_and_geolite2_reader() {
        try (final MockedConstruction<HttpDBDownloadService> httpDBDownloadServiceMockedConstruction = mockConstruction(HttpDBDownloadService.class,
                     (mock2, context2)-> doNothing().when(mock2).initiateDownload());
            final MockedConstruction<GeoLite2DatabaseReader> geoLite2DatabaseReaderMockedConstruction = mockConstruction(GeoLite2DatabaseReader.class);
            final MockedConstruction<AutoCountingDatabaseReader> autoCountingDatabaseReaderMockedConstruction = mockConstruction(AutoCountingDatabaseReader.class)
        ) {

            final HashMap<String, String> databases = new HashMap<>();
            databases.put("geolite2_country", HTTPS_URL);
            when(maxMindDatabaseConfig.getDatabasePaths()).thenReturn(databases);
            when(maxMindConfig.getMaxMindDatabaseConfig()).thenReturn(maxMindDatabaseConfig);
            final GeoIPDatabaseManager objectUnderTest = createObjectUnderTest();
            objectUnderTest.initiateDatabaseDownload();

            assertThat(httpDBDownloadServiceMockedConstruction.constructed().size(), equalTo(1));
            verify(httpDBDownloadServiceMockedConstruction.constructed().get(0)).initiateDownload();
            assertThat(geoLite2DatabaseReaderMockedConstruction.constructed().size(), equalTo(1));
            assertThat(autoCountingDatabaseReaderMockedConstruction.constructed().size(), equalTo(1));

            final GeoIPDatabaseReader geoIPDatabaseReader = objectUnderTest.getGeoIPDatabaseReader();
            assertThat(geoIPDatabaseReader, equalTo(autoCountingDatabaseReaderMockedConstruction.constructed().get(0)));
        }
    }

    @Test
    void test_initiateDatabaseDownload_with_geoip2_url_should_use_http_download_service_and_geoip2_reader() {
        try (final MockedConstruction<HttpDBDownloadService> httpDBDownloadServiceMockedConstruction = mockConstruction(HttpDBDownloadService.class,
                     (mock2, context2)-> doNothing().when(mock2).initiateDownload());
             final MockedConstruction<GeoIP2DatabaseReader> geoIP2DatabaseReaderMockedConstruction = mockConstruction(GeoIP2DatabaseReader.class);
             final MockedConstruction<AutoCountingDatabaseReader> autoCountingDatabaseReaderMockedConstruction = mockConstruction(AutoCountingDatabaseReader.class)
        ) {
            when(licenseTypeCheck.isGeoLite2OrEnterpriseLicense(any())).thenReturn(LicenseTypeOptions.ENTERPRISE);
            final HashMap<String, String> databases = new HashMap<>();
            databases.put("geolite2_country", HTTPS_URL);
            when(maxMindDatabaseConfig.getDatabasePaths()).thenReturn(databases);
            when(maxMindConfig.getMaxMindDatabaseConfig()).thenReturn(maxMindDatabaseConfig);

            final GeoIPDatabaseManager objectUnderTest = createObjectUnderTest();
            objectUnderTest.initiateDatabaseDownload();

            assertThat(httpDBDownloadServiceMockedConstruction.constructed().size(), equalTo(1));
            verify(httpDBDownloadServiceMockedConstruction.constructed().get(0)).initiateDownload();
            assertThat(geoIP2DatabaseReaderMockedConstruction.constructed().size(), equalTo(1));
            assertThat(autoCountingDatabaseReaderMockedConstruction.constructed().size(), equalTo(1));

            final GeoIPDatabaseReader geoIPDatabaseReader = objectUnderTest.getGeoIPDatabaseReader();
            assertThat(geoIPDatabaseReader, equalTo(autoCountingDatabaseReaderMockedConstruction.constructed().get(0)));
        }
    }

    @Test
    void test_initiateDatabaseDownload_with_geolite2_cdn_should_use_cdn_download_service_and_geolite2_reader() {
        try (final MockedConstruction<ManifestDownloadService> cdnDownloadServiceMockedConstruction = mockConstruction(ManifestDownloadService.class,
                     (mock2, context2)-> doNothing().when(mock2).initiateDownload());
             final MockedConstruction<GeoLite2DatabaseReader> geoLite2DatabaseReaderMockedConstruction = mockConstruction(GeoLite2DatabaseReader.class);
             final MockedConstruction<AutoCountingDatabaseReader> autoCountingDatabaseReaderMockedConstruction = mockConstruction(AutoCountingDatabaseReader.class)
        ) {
            final HashMap<String, String> databases = new HashMap<>();
            databases.put("geolite2_country", CDN_ENDPOINT);
            when(maxMindDatabaseConfig.getDatabasePaths()).thenReturn(databases);
            when(maxMindConfig.getMaxMindDatabaseConfig()).thenReturn(maxMindDatabaseConfig);
            final GeoIPDatabaseManager objectUnderTest = createObjectUnderTest();
            objectUnderTest.initiateDatabaseDownload();

            assertThat(cdnDownloadServiceMockedConstruction.constructed().size(), equalTo(1));
            verify(cdnDownloadServiceMockedConstruction.constructed().get(0)).initiateDownload();
            assertThat(geoLite2DatabaseReaderMockedConstruction.constructed().size(), equalTo(1));
            assertThat(autoCountingDatabaseReaderMockedConstruction.constructed().size(), equalTo(1));

            final GeoIPDatabaseReader geoIPDatabaseReader = objectUnderTest.getGeoIPDatabaseReader();
            assertThat(geoIPDatabaseReader, equalTo(autoCountingDatabaseReaderMockedConstruction.constructed().get(0)));
        }
    }

    @Test
    void test_updateDatabaseReader_with_geolite2_cdn_should_use_cdn_download_service_and_geolite2_reader_and_get_new_reader() throws Exception {
        try (final MockedConstruction<ManifestDownloadService> cdnDownloadServiceMockedConstruction = mockConstruction(ManifestDownloadService.class,
                     (mock2, context2)-> doNothing().when(mock2).initiateDownload());
             final MockedConstruction<GeoLite2DatabaseReader> geoLite2DatabaseReaderMockedConstruction = mockConstruction(GeoLite2DatabaseReader.class);
             final MockedConstruction<AutoCountingDatabaseReader> autoCountingDatabaseReaderMockedConstruction = mockConstruction(AutoCountingDatabaseReader.class)
        ) {
            final HashMap<String, String> databases = new HashMap<>();
            databases.put("geolite2_country", CDN_ENDPOINT);
            when(maxMindDatabaseConfig.getDatabasePaths()).thenReturn(databases);
            when(maxMindConfig.getMaxMindDatabaseConfig()).thenReturn(maxMindDatabaseConfig);
            final GeoIPDatabaseManager objectUnderTest = createObjectUnderTest();

            objectUnderTest.initiateDatabaseDownload();
            final GeoIPDatabaseReader geoIPDatabaseReader = objectUnderTest.getGeoIPDatabaseReader();
            assertThat(geoIPDatabaseReader, equalTo(autoCountingDatabaseReaderMockedConstruction.constructed().get(0)));

            objectUnderTest.updateDatabaseReader();

            assertThat(cdnDownloadServiceMockedConstruction.constructed().size(), equalTo(2));
            for (ManifestDownloadService manifestDownloadService : cdnDownloadServiceMockedConstruction.constructed()) {
                verify(manifestDownloadService).initiateDownload();
            }
            assertThat(geoLite2DatabaseReaderMockedConstruction.constructed().size(), equalTo(2));
            assertThat(autoCountingDatabaseReaderMockedConstruction.constructed().size(), equalTo(2));
            // verify if first instance is closed
            verify(autoCountingDatabaseReaderMockedConstruction.constructed().get(0)).close();
            final GeoIPDatabaseReader updatedGeoIPDatabaseReader = objectUnderTest.getGeoIPDatabaseReader();
            assertThat(updatedGeoIPDatabaseReader, equalTo(autoCountingDatabaseReaderMockedConstruction.constructed().get(1)));
        }
    }

    @Test
    void test_initiateDatabaseDownload_with_exception_should_update_nextUpdateAt_correctly_with_backoff() {
        try (final MockedConstruction<ManifestDownloadService> cdnDownloadServiceMockedConstruction = mockConstruction(ManifestDownloadService.class,
                (mock2, context2)-> doThrow(DownloadFailedException.class).when(mock2).initiateDownload());
             final MockedConstruction<GeoLite2DatabaseReader> geoLite2DatabaseReaderMockedConstruction = mockConstruction(GeoLite2DatabaseReader.class);
             final MockedConstruction<AutoCountingDatabaseReader> autoCountingDatabaseReaderMockedConstruction = mockConstruction(AutoCountingDatabaseReader.class)
        ) {
            final HashMap<String, String> databases = new HashMap<>();
            databases.put("geolite2_country", CDN_ENDPOINT);
            when(maxMindDatabaseConfig.getDatabasePaths()).thenReturn(databases);
            when(maxMindConfig.getMaxMindDatabaseConfig()).thenReturn(maxMindDatabaseConfig);
            final GeoIPDatabaseManager objectUnderTest = createObjectUnderTest();

            assertThrows(DownloadFailedException.class, objectUnderTest::initiateDatabaseDownload);

            assertThat(cdnDownloadServiceMockedConstruction.constructed().size(), equalTo(1));
            verify(cdnDownloadServiceMockedConstruction.constructed().get(0)).initiateDownload();
            assertThat(geoLite2DatabaseReaderMockedConstruction.constructed().size(), equalTo(0));
            assertThat(autoCountingDatabaseReaderMockedConstruction.constructed().size(), equalTo(0));

            final GeoIPDatabaseReader geoIPDatabaseReader = objectUnderTest.getGeoIPDatabaseReader();
            assertThat(geoIPDatabaseReader, equalTo(null));

            assertTrue(Instant.now().plus(Duration.ofMinutes(5)).isAfter(objectUnderTest.getNextUpdateAt()));
            assertTrue(Instant.now().minus(Duration.ofMinutes(5)).isBefore(objectUnderTest.getNextUpdateAt()));
        }
    }

    @Test
    void test_initiateDatabaseDownload_without_exception_should_update_databases_as_configured() {
        try (final MockedConstruction<ManifestDownloadService> cdnDownloadServiceMockedConstruction = mockConstruction(ManifestDownloadService.class,
                (mock2, context2)-> doNothing().when(mock2).initiateDownload());
             final MockedConstruction<GeoLite2DatabaseReader> geoLite2DatabaseReaderMockedConstruction = mockConstruction(GeoLite2DatabaseReader.class);
             final MockedConstruction<AutoCountingDatabaseReader> autoCountingDatabaseReaderMockedConstruction = mockConstruction(AutoCountingDatabaseReader.class)
        ) {
            final HashMap<String, String> databases = new HashMap<>();
            databases.put("geolite2_country", CDN_ENDPOINT);
            when(maxMindDatabaseConfig.getDatabasePaths()).thenReturn(databases);
            when(maxMindConfig.getMaxMindDatabaseConfig()).thenReturn(maxMindDatabaseConfig);
            when(maxMindConfig.getDatabaseRefreshInterval()).thenReturn(Duration.ofDays(3));
            final GeoIPDatabaseManager objectUnderTest = createObjectUnderTest();

            objectUnderTest.initiateDatabaseDownload();

            assertThat(cdnDownloadServiceMockedConstruction.constructed().size(), equalTo(1));
            verify(cdnDownloadServiceMockedConstruction.constructed().get(0)).initiateDownload();
            assertThat(geoLite2DatabaseReaderMockedConstruction.constructed().size(), equalTo(1));
            assertThat(autoCountingDatabaseReaderMockedConstruction.constructed().size(), equalTo(1));

            final GeoIPDatabaseReader geoIPDatabaseReader = objectUnderTest.getGeoIPDatabaseReader();
            assertThat(geoIPDatabaseReader, equalTo(autoCountingDatabaseReaderMockedConstruction.constructed().get(0)));

            assertTrue(Instant.now().plus(Duration.ofDays(4)).isAfter(objectUnderTest.getNextUpdateAt()));
            assertTrue(Instant.now().minus(Duration.ofDays(2)).isBefore(objectUnderTest.getNextUpdateAt()));
        }
    }

    @Test
    void test_getNextUpdateAt() throws NoSuchFieldException, IllegalAccessException {
        final GeoIPDatabaseManager objectUnderTest = createObjectUnderTest();

        ReflectivelySetField.setField(GeoIPDatabaseManager.class, objectUnderTest, "nextUpdateAt", Instant.now());

        assertTrue(objectUnderTest.getNextUpdateAt().isBefore(Instant.now()));
        assertTrue(objectUnderTest.getNextUpdateAt().plus(Duration.ofMinutes(1)).isAfter(Instant.now()));
    }
}
