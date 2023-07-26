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
    private String  maxmindLicenseKey;
    private GeoIPProcessorService geoIPProcessorService;
    private GeoIPInputJson geoIPInputJson;
    private String jsonInput;
    private static final String TEMP_PATH_FOLDER = "GeoIP";
    public static final String DATABASE_1 = "first_database";
    public static final String URL_SUFFIX = "&suffix=tar.gz";

    @BeforeEach
    public void setUp() throws JsonProcessingException {

        maxmindLicenseKey = "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";

        jsonInput = "{\"peer\": { \"ips\":{ \"src_ip1\" : \"8.8.8.8\", \"dst_ip1\": \"8.8.8.9\" }, \"host\": \"example.org\" }, \"status\": \"success\"}";

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
                "              source: [ \"/peer/ips/src_ip1\",\"/peer/ips/dst_ip1\" ]\n" +
                "              target: [ \"target1\",\"target2\" ]\n" +
                "          - key:\n" +
                "              source: [\"/peer/ips/src_ip2\"]\n" +
                "              target: [\"target2\"]\n" +
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

        String src_IpAddress = geoIPInputJson.getPeer().getIps().getSrc_ip1();
        if (IPValidationcheck.isPublicIpAddress(src_IpAddress)) {
            InetAddress src_InetAddress = InetAddress.getByName(src_IpAddress);
            //All attributes are considered by default with null
            geoData = geoIPProcessorService.getGeoData(src_InetAddress, null);

            assertThat(geoData.get("country_iso_code"), equalTo("US"));
            assertThat(geoData.get("ip"), equalTo("8.8.8.8"));
            assertThat(geoData.get("country_name"), equalTo("United States"));
            assertThat(geoData.get("organization_name"), equalTo("GOOGLE"));
        }

        String dest_IpAddress = geoIPInputJson.getPeer().getIps().getDst_ip1();
        if (IPValidationcheck.isPublicIpAddress(dest_IpAddress)) {
            InetAddress dest_InetAddress = InetAddress.getByName(dest_IpAddress);
            //All attributes are considered by default with null
            geoData = geoIPProcessorService.getGeoData(dest_InetAddress, null);

            assertThat(geoData.get("country_iso_code"), equalTo("US"));
            assertThat(geoData.get("ip"), equalTo("8.8.8.9"));
            assertThat(geoData.get("country_name"), equalTo("United States"));
            assertThat(geoData.get("organization_name"), equalTo("GOOGLE"));
        }
    }
}