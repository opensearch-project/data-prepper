/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.databaseenrich;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.processor.GeoIPProcessorConfig;
import org.opensearch.dataprepper.plugins.processor.GeoIPProcessorService;
import org.opensearch.dataprepper.plugins.processor.databasedownload.DBSource;
import org.opensearch.dataprepper.plugins.processor.extension.GeoIpServiceConfig;
import org.opensearch.dataprepper.plugins.processor.extension.MaxMindConfig;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GetGeoIP2DataTest {

    private static final String PATH = "./src/test/resources/mmdb-file/geo-enterprise";
    private String tempFolderPath = System.getProperty("java.io.tmpdir") + File.separator + "GeoIP";
    private static final String PREFIX_DIR = "first_database";
    public static final int REFRESH_SCHEDULE = 10;
    public static final String IP = "2001:4860:4860::8888";
    @Mock
    private GeoIPProcessorConfig geoIPProcessorConfig;
    @Mock
    private GeoIpServiceConfig geoIpServiceConfig;
    @Mock
    private MaxMindConfig maxMindConfig;
    @Mock
    private DBSource downloadSource;
    private GetGeoIP2Data getGeoIP2Data;
    private final int cacheSize = 4068;

    @BeforeEach
    void setUp() {
        when(geoIpServiceConfig.getMaxMindConfig())
                .thenReturn(maxMindConfig);
        String dbPath = "./src/test/resources/mmdb-file/geo-enterprise";
        getGeoIP2Data = new GetGeoIP2Data(dbPath, cacheSize);
    }

    @Disabled("Doesn't have valid GeoIP2-Enterprise.mmdb")
    @Test
    void getGeoDataTest() throws UnknownHostException {

        List<String> attributes = List.of("city_name", "country_name");
        InetAddress inetAddress = InetAddress.getByName(IP);
        GeoIPProcessorService.downloadReady = false;
        Map<String, Object> geoData = getGeoIP2Data.getGeoData(inetAddress, attributes, PREFIX_DIR);
        assertThat(geoData.get("country_iso_code"), equalTo("US"));
        assertThat(geoData.get("ip"), equalTo("2001:4860:4860:0:0:0:0:8888"));
    }

    @Test
    @Disabled
    void switchDatabaseReaderTest() {
        tempFolderPath = System.getProperty("java.io.tmpdir") + File.separator + "GeoIPMaxmind";
        DBSource.createFolderIfNotExist(tempFolderPath);
        getGeoIP2Data = new GetGeoIP2Data(tempFolderPath, cacheSize);
        assertDoesNotThrow(() -> {
            getGeoIP2Data.switchDatabaseReader();
        });
        assertDoesNotThrow(() -> {
            getGeoIP2Data.closeReader();
        });
    }

    @Test
    @Disabled
    void getGeoDataTest_cover_EnrichFailedException() throws UnknownHostException {
        List<String> attributes = List.of("city_name", "country_name");
        InetAddress inetAddress = InetAddress.getByName(IP);
        GeoIPProcessorService.downloadReady = false;
        assertThrows(EnrichFailedException.class, () -> getGeoIP2Data.getGeoData(inetAddress, attributes, PREFIX_DIR));
    }

    @Test
    @Disabled
    void closeReaderTest() throws UnknownHostException {
        getGeoIP2Data.closeReader();
        List<String> attributes = List.of("city_name", "country_name");
        InetAddress inetAddress = InetAddress.getByName(IP);
        assertThrows(EnrichFailedException.class, () -> getGeoIP2Data.getGeoData(inetAddress, attributes, PREFIX_DIR));
    }
}