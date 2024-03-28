/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.extension;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.io.File;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MaxMindConfigTest {
    @Mock
    private MaxMindDatabaseConfig maxMindDatabaseConfig;

    private MaxMindConfig createObjectUnderTest() {
        return new MaxMindConfig();
    }

    @RepeatedTest(2)
    void testDefaultConfig() {
        final String dataPrepperDirectory = UUID.randomUUID().toString();
        System.setProperty("data-prepper.dir", dataPrepperDirectory);
        final MaxMindConfig maxMindConfig = createObjectUnderTest();
        assertThat(maxMindConfig.getDatabaseRefreshInterval(), equalTo(Duration.ofDays(7)));
        assertThat(maxMindConfig.getCacheSize(), equalTo(4096));
        assertThat(maxMindConfig.getAwsAuthenticationOptionsConfig(), equalTo(null));
        assertThat(maxMindConfig.getDatabaseDestination(), equalTo(dataPrepperDirectory + File.separator + "data" + File.separator + "geoip"));
        assertThat(maxMindConfig.getMaxMindDatabaseConfig(), instanceOf(MaxMindDatabaseConfig.class));
    }

    @Test
    void testCustomConfig() throws NoSuchFieldException, IllegalAccessException {
        final MaxMindConfig maxMindConfig = createObjectUnderTest();
        ReflectivelySetField.setField(MaxMindConfig.class, maxMindConfig, "databaseRefreshInterval", Duration.ofDays(10));
        ReflectivelySetField.setField(MaxMindConfig.class, maxMindConfig, "cacheSize", 2048);
        ReflectivelySetField.setField(MaxMindConfig.class, maxMindConfig, "maxMindDatabaseConfig", maxMindDatabaseConfig);
        ReflectivelySetField.setField(MaxMindConfig.class, maxMindConfig, "databaseDestination", "/data");

        final AwsAuthenticationOptionsConfig awsAuthenticationOptionsConfig = new AwsAuthenticationOptionsConfig();
        ReflectivelySetField.setField(MaxMindConfig.class, maxMindConfig, "awsAuthenticationOptionsConfig", awsAuthenticationOptionsConfig);

        assertThat(maxMindConfig.getDatabaseRefreshInterval(), equalTo(Duration.ofDays(10)));
        assertThat(maxMindConfig.getCacheSize(), equalTo(2048));
        assertThat(maxMindConfig.getDatabaseDestination(), equalTo("/data"));
        assertThat(maxMindConfig.getAwsAuthenticationOptionsConfig(), equalTo(awsAuthenticationOptionsConfig));
        assertThat(maxMindConfig.getMaxMindDatabaseConfig(), equalTo(maxMindDatabaseConfig));
    }

    @ParameterizedTest
    @CsvSource({
            "https://download.maxmind.com/, false, true",
            "http://download.maxmind.com/, false, false",
            "https://download.maxmind.com/, true, true",
            "http://download.maxmind.com/, true, true"})
    void testSecureEndpoint(final String databasePath, final boolean insecure, final boolean result)
            throws NoSuchFieldException, IllegalAccessException, URISyntaxException {
        final MaxMindConfig maxMindConfig = createObjectUnderTest();
        when(maxMindDatabaseConfig.getDatabasePaths()).thenReturn(Map.of("name", databasePath));
        ReflectivelySetField.setField(MaxMindConfig.class, maxMindConfig, "maxMindDatabaseConfig", maxMindDatabaseConfig);
        ReflectivelySetField.setField(MaxMindConfig.class, maxMindConfig, "insecure", insecure);

        assertThat(maxMindConfig.getMaxMindDatabaseConfig().getDatabasePaths().size(), equalTo(1));
        assertThat(maxMindConfig.isHttpsEndpointOrInsecure(), equalTo(result));
    }

    @ParameterizedTest
    @CsvSource({
            "s3://geoip/data, false, false",
            "s3://geoip/data, true, true"})
    void testValidPaths(final String databasePath, final boolean awsConfig, final boolean result)
            throws NoSuchFieldException, IllegalAccessException {
        final MaxMindConfig maxMindConfig = createObjectUnderTest();
        when(maxMindDatabaseConfig.getDatabasePaths()).thenReturn(Map.of("name", databasePath));
        ReflectivelySetField.setField(MaxMindConfig.class, maxMindConfig, "maxMindDatabaseConfig", maxMindDatabaseConfig);
        if (awsConfig) {
            final AwsAuthenticationOptionsConfig awsAuthenticationOptionsConfig = new AwsAuthenticationOptionsConfig();
            ReflectivelySetField.setField(MaxMindConfig.class, maxMindConfig, "awsAuthenticationOptionsConfig", awsAuthenticationOptionsConfig);
        }

        assertThat(maxMindConfig.getMaxMindDatabaseConfig().getDatabasePaths().size(), equalTo(1));
        assertThat(maxMindConfig.isAwsAuthenticationOptionsValid(), equalTo(result));
    }
}