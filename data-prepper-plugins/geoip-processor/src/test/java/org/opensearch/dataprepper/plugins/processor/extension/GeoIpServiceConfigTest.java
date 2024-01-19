/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.parser.DataPrepperDurationDeserializer;
import org.opensearch.dataprepper.parser.model.DataPrepperConfiguration;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.time.Duration;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class GeoIpServiceConfigTest {
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper(new YAMLFactory());

        final SimpleModule simpleModule = new SimpleModule()
                .addDeserializer(Duration.class, new DataPrepperDurationDeserializer());
        objectMapper.registerModule(simpleModule);
    }

    @Test
    void testGeoIpServiceConfig() throws IOException {
        final GeoIpServiceConfig geoIpServiceConfig = makeConfig("src/test/resources/geoip_service_config.yaml");
        assertNotNull(geoIpServiceConfig);

        final MaxMindConfig maxMindConfig = geoIpServiceConfig.getMaxMindConfig();
        assertNotNull(maxMindConfig);
        assertNotNull(maxMindConfig.getAwsAuthenticationOptionsConfig());

        assertThat(maxMindConfig, notNullValue());
        assertThat(maxMindConfig.getDatabaseRefreshInterval(), equalTo(Duration.ofDays(10)));
        assertThat(maxMindConfig.getDatabasePaths().size(), equalTo(2));
        assertThat(maxMindConfig.getCacheSize(), equalTo(2048));
    }

    private GeoIpServiceConfig makeConfig(final String filePath) throws IOException {
        final File configurationFile = new File(filePath);
        final DataPrepperConfiguration dataPrepperConfiguration = objectMapper.readValue(configurationFile, DataPrepperConfiguration.class);

        assertThat(dataPrepperConfiguration, CoreMatchers.notNullValue());
        assertThat(dataPrepperConfiguration.getPipelineExtensions(), CoreMatchers.notNullValue());

        final Map<String, Object> geoipServiceConfigMap = (Map<String, Object>) dataPrepperConfiguration.getPipelineExtensions().getExtensionMap().get("geoip_service");
        String json = objectMapper.writeValueAsString(geoipServiceConfigMap);
        final Reader reader = new StringReader(json);
        return objectMapper.readValue(reader, GeoIpServiceConfig.class);
    }

}