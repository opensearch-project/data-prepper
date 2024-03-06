/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.extension.databasedownload;

import com.maxmind.geoip2.DatabaseReader;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
class DatabaseReaderBuilderTest {
    public static final String GEOLITE2_TEST_MMDB_FILES = "./build/resources/test/mmdb-files/geo-lite2";
    public static final String GEOIP2_TEST_MMDB_FILES = "./build/resources/test/mmdb-files/geo-ip2";

    @ParameterizedTest
    @ValueSource(strings = {"geolite2-asn", "geolite2-city", "geolite2-country"})
    void createLoaderTest_for_geolite2_databases(final String databaseName) throws IOException {
        String databaseToUse = null;
        final File directory = new File(GEOLITE2_TEST_MMDB_FILES);
        final String[] list = directory.list();
        for (String fileName: list) {
            final String lowerCaseFileName = fileName.toLowerCase();
            if (fileName.endsWith(".mmdb")
                    && lowerCaseFileName.contains(databaseName)) {
                databaseToUse = fileName;
            }
        }

        final Path path = Path.of(GEOLITE2_TEST_MMDB_FILES + File.separator + databaseToUse);

        final DatabaseReaderBuilder databaseReaderBuilder = new DatabaseReaderBuilder();

        final DatabaseReader databaseReader = databaseReaderBuilder.buildReader(path, 4096);
        assertNotNull(databaseReader);
        assertTrue(databaseToUse.toLowerCase().contains(databaseReader.getMetadata().getDatabaseType().toLowerCase()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"geoip2-enterprise"})
    void createLoaderTest_for_geoip2_databases(final String databaseName) throws IOException {
        String databaseToUse = null;
        final File directory = new File(GEOIP2_TEST_MMDB_FILES);
        final String[] list = directory.list();
        for (String fileName: list) {
            final String lowerCaseFileName = fileName.toLowerCase();
            if (fileName.endsWith(".mmdb")
                    && lowerCaseFileName.contains(databaseName)) {
                databaseToUse = fileName;
            }
        }

        final Path path = Path.of(GEOIP2_TEST_MMDB_FILES + File.separator + databaseToUse);

        final DatabaseReaderBuilder databaseReaderBuilder = new DatabaseReaderBuilder();

        final DatabaseReader databaseReader = databaseReaderBuilder.buildReader(path, 4096);
        assertNotNull(databaseReader);
        assertTrue(databaseToUse.toLowerCase().contains(databaseReader.getMetadata().getDatabaseType().toLowerCase()));
    }
}
