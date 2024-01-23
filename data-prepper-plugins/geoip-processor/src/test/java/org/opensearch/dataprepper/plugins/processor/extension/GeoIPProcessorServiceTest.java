/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.processor.GeoIPProcessorConfig;
import org.opensearch.dataprepper.plugins.processor.databaseenrich.GetGeoData;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@Disabled
class GeoIPProcessorServiceTest {

    private static final String S3_URL = "https://mybucket10012023.s3.amazonaws.com/GeoLite2/";
    private static final String URL = "https://download.maxmind.com/app/geoip_download?edition_" +
            "id=GeoLite2-ASN&suffix=tar.gz";
    private static final String PATH = "./src/test/resources/mmdb-file/geo-lite2";
    private static final String S3_REGION = "us-east-1";
    public static final int REFRESH_SCHEDULE = 10;
    public static final String IP = "2001:4860:4860::8888";
    String tempFolderPath = System.getProperty("java.io.tmpdir") + File.separator + "GeoIP";
    @Mock
    private GeoIPProcessorConfig geoIPProcessorConfig;
    @Mock
    private GetGeoData geoData;
    @Mock
    private MaxMindConfig maxMindConfig;
    private GeoIPProcessorService geoIPProcessorService;

    @BeforeEach
    void setUp() {

    }

    @Test
    void getGeoDataTest_PATH() throws UnknownHostException, NoSuchFieldException, IllegalAccessException {
        // TODO: pass in geoIpServiceConfig object
        geoIPProcessorService = new GeoIPProcessorService(null);

        List<String> attributes = List.of();
        InetAddress inetAddress = InetAddress.getByName(IP);
        ReflectivelySetField.setField(GeoIPProcessorService.class,
                geoIPProcessorService, "geoData", geoData);
        when(geoData.getGeoData(any(), any(), anyString())).thenReturn(prepareGeoData());
        Map<String, Object> geoData = geoIPProcessorService.getGeoData(inetAddress, attributes);
        assertThat(geoData.get("country_iso_code"), equalTo("US"));
        assertThat(geoData.get("continent_name"), equalTo("North America"));
    }

    @Test
    void getGeoDataTest_URL() throws UnknownHostException, NoSuchFieldException, IllegalAccessException {
        // TODO: pass in geoIpServiceConfig object
        geoIPProcessorService = new GeoIPProcessorService(null);

        List<String> attributes = List.of();
        InetAddress inetAddress = InetAddress.getByName(IP);
        ReflectivelySetField.setField(GeoIPProcessorService.class,
                geoIPProcessorService, "geoData", geoData);
        when(geoData.getGeoData(any(), any(), anyString())).thenReturn(prepareGeoData());
        Map<String, Object> geoData = geoIPProcessorService.getGeoData(inetAddress, attributes);
        assertThat(geoData.get("country_iso_code"), equalTo("US"));
        assertThat(geoData.get("continent_name"), equalTo("North America"));
    }

    private Map<String, Object> prepareGeoData() {
        Map<String, Object> geoDataMap = new HashMap<>();
        geoDataMap.put("country_iso_code", "US");
        geoDataMap.put("continent_name", "North America");
        geoDataMap.put("timezone", "America/Chicago");
        geoDataMap.put("country_name", "United States");
        return geoDataMap;
    }
}