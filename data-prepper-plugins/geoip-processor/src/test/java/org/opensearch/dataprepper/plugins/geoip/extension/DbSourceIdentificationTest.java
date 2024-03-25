/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.extension;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.geoip.extension.databasedownload.DBSourceOptions;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DbSourceIdentificationTest {

    private static final String S3_URI = "s3://dataprepper/logdata/22833bd46b8e0.mmdb";
    private static final String URL = "https://download.maxmind.com/";
    private static final String DIRECTORY_PATH = "./build/resources/test/mmdb-files/geo-lite2";
    private static final String FILE_PATH = "./build/resources/test/mmdb-files/geo-lite2/GeoLite2-ASN-Test.mmdb";
    private static final String CDN_ENDPOINT_HOST = "https://devo.geoip.maps.opensearch.org/v1/mmdb/geolite2/manifest.json";

    @Test
    void test_positive_case() {
        assertTrue(DatabaseSourceIdentification.isS3Uri(S3_URI));
        assertTrue(DatabaseSourceIdentification.isURL(URL));
        assertTrue(DatabaseSourceIdentification.isFilePath(FILE_PATH));
        assertTrue(DatabaseSourceIdentification.isCDNEndpoint(CDN_ENDPOINT_HOST));
    }

    @Test
    void test_negative_case() {
        assertFalse(DatabaseSourceIdentification.isS3Uri(CDN_ENDPOINT_HOST));
        assertFalse(DatabaseSourceIdentification.isS3Uri(URL));
        assertFalse(DatabaseSourceIdentification.isS3Uri(DIRECTORY_PATH));

        assertFalse(DatabaseSourceIdentification.isURL(S3_URI));
        assertFalse(DatabaseSourceIdentification.isURL(CDN_ENDPOINT_HOST));
        assertFalse(DatabaseSourceIdentification.isURL(DIRECTORY_PATH));

        assertFalse(DatabaseSourceIdentification.isFilePath(S3_URI));
        assertFalse(DatabaseSourceIdentification.isFilePath(CDN_ENDPOINT_HOST));
        assertFalse(DatabaseSourceIdentification.isFilePath(URL));

        assertFalse(DatabaseSourceIdentification.isCDNEndpoint(S3_URI));
        assertFalse(DatabaseSourceIdentification.isCDNEndpoint(DIRECTORY_PATH));
        assertFalse(DatabaseSourceIdentification.isCDNEndpoint(URL));
    }

    @Test
    void getDatabasePathTypeTest_should_return_PATH_if_file() {
        List<String> databasePath = List.of(FILE_PATH);
        DBSourceOptions dbSourceOptions = DatabaseSourceIdentification.getDatabasePathType(databasePath);
        Assertions.assertNotNull(dbSourceOptions);
        assertThat(dbSourceOptions, equalTo(DBSourceOptions.PATH));
    }

    @Test
    void getDatabasePathTypeTest_should_null_if_directory() {
        List<String> databasePath = List.of(DIRECTORY_PATH);
        DBSourceOptions dbSourceOptions = DatabaseSourceIdentification.getDatabasePathType(databasePath);
        Assertions.assertNull(dbSourceOptions);
    }

    @Test
    void getDatabasePathTypeTest_URL() {
        List<String> databasePath = List.of("https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-ASN&suffix=tar.gz");
        DBSourceOptions dbSourceOptions = DatabaseSourceIdentification.getDatabasePathType(databasePath);
        Assertions.assertNotNull(dbSourceOptions);
        assertThat(dbSourceOptions, equalTo(DBSourceOptions.URL));
    }

    @Test
    void getDatabasePathTypeTest_S3() {
        List<String> databasePath = List.of("s3://mybucket10012023/GeoLite2/");
        DBSourceOptions dbSourceOptions = DatabaseSourceIdentification.getDatabasePathType(databasePath);
        Assertions.assertNotNull(dbSourceOptions);
        assertThat(dbSourceOptions, equalTo(DBSourceOptions.S3));
    }

    @Test
    void getDatabasePathTypeTest_CDN() {
        List<String> databasePath = List.of(CDN_ENDPOINT_HOST);
        DBSourceOptions dbSourceOptions = DatabaseSourceIdentification.getDatabasePathType(databasePath);
        Assertions.assertNotNull(dbSourceOptions);
        assertThat(dbSourceOptions, equalTo(DBSourceOptions.HTTP_MANIFEST));
    }

    @Test
    void isS3Uri_NullPointerException_test() {
        assertDoesNotThrow(() -> {
            DatabaseSourceIdentification.isS3Uri(null);
        });
    }
}