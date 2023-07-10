/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.processor.utils.IPValidationcheck;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@ExtendWith(MockitoExtension.class)
public class GeoIPProcessorUrlServiceIT {

    private ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));
    private String tempPath;
    private GeoIPProcessorConfig geoIPProcessorConfig;
    private String ipAddress;
    private String  maxmindLicenseKey;
    private GeoIPProcessorService geoIPProcessorService;
    private GeoIPInputJson geoIPInputJson;
    private String jsonInput;
    private static final String TEMP_PATH_FOLDER = "GeoIP";
    public static final String DATABASE_1 = "first_database";
    public static final String URL_SUFFIX = "&suffix=tar.gz";

    @BeforeEach
    public void setUp() throws JsonProcessingException {

        maxmindLicenseKey = System.getProperty("tests.geoipProcessor.maxmindLicenseKey");

        jsonInput = "{\"peer\": {\"ip\": \"8.8.8.8\", \"host\": \"example.org\" }, \"status\": \"success\"}";

        geoIPInputJson = objectMapper.readValue(jsonInput, GeoIPInputJson.class);
        tempPath = System.getProperty("java.io.tmpdir")+ File.separator + TEMP_PATH_FOLDER;

        String asnUrl = "https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-ASN&license_key=" + maxmindLicenseKey + URL_SUFFIX;
        String cityUrl = "https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-City&license_key=" + maxmindLicenseKey + URL_SUFFIX;
        String countryUrl = "https://download.maxmind.com/app/geoip_download?edition_id=GeoLite2-Country&license_key=" + maxmindLicenseKey + URL_SUFFIX;

        String pipelineConfig = "        aws:\n" +
                "          region: us-east-2\n" +
                "          sts_role_arn: \"arn:aws:iam::123456789:role/data-prepper-execution-role\"\n" +
                "        keys:\n" +
                "          - key:\n" +
                "              source: \"/peer/ip\"\n" +
                "              target: \"target1\"\n" +
                "          - key:\n" +
                "              source: \"/peer/ip2\"\n" +
                "              target: \"target2\"\n" +
                "              attributes: [\"city_name\",\"country_name\"]\n" +
                "        service_type:\n" +
                "          maxmind:\n" +
                "            database_path:\n" +
                "              - url: " + asnUrl + "\n" +
                "              - url: " + cityUrl + "\n" +
                "              - url: " + countryUrl + "\n" +
                "            load_type: \"cache\"\n" +
                "            cache_size: 8192\n" +
                "            cache_refresh_schedule: PT3M";

        objectMapper.registerModule(new JavaTimeModule());
        this.geoIPProcessorConfig = objectMapper.readValue(pipelineConfig, GeoIPProcessorConfig.class);
    }

    public GeoIPProcessorService createObjectUnderTest() {
        return new GeoIPProcessorService(geoIPProcessorConfig, tempPath);
    }

    @Test
    void verify_enrichment_of_data_from_maxmind_url() throws UnknownHostException {

        Map<String, Object> geoData = new HashMap<>();
        this.geoIPProcessorService = createObjectUnderTest();
        String ipAddress = geoIPInputJson.getPeer().getIp();
        if (IPValidationcheck.isPublicIpAddress(ipAddress)) {
            InetAddress inetAddress = InetAddress.getByName(ipAddress);
            //All attributes are considered by default with the null value
            geoData = geoIPProcessorService.getGeoData(inetAddress, null);

            assertThat(geoData.get("country_iso_code"), equalTo("US"));
            assertThat(geoData.get("ip"), equalTo("8.8.8.8"));
            assertThat(geoData.get("country_name"), equalTo("United States"));
            assertThat(geoData.get("organization_name"), equalTo("GOOGLE"));
        }
    }
}