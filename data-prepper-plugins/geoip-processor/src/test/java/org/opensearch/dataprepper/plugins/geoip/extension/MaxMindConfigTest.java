/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.extension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.geoip.extension.MaxMindConfig.DEFAULT_DATABASE_DESTINATION;

@ExtendWith(MockitoExtension.class)
class MaxMindConfigTest {
    private MaxMindConfig maxMindConfig;
    @Mock
    private MaxMindDatabaseConfig maxMindDatabaseConfig;

    @BeforeEach
    void setup() {
        maxMindConfig = new MaxMindConfig();
    }

    @Test
    void testDefaultConfig() {
        assertThat(maxMindConfig.getDatabaseRefreshInterval(), equalTo(Duration.ofDays(7)));
        assertThat(maxMindConfig.getCacheSize(), equalTo(4096));
        assertThat(maxMindConfig.getAwsAuthenticationOptionsConfig(), equalTo(null));
        assertThat(maxMindConfig.getDatabaseDestination(), equalTo(DEFAULT_DATABASE_DESTINATION));
        assertThat(maxMindConfig.getMaxMindDatabaseConfig(), instanceOf(MaxMindDatabaseConfig.class));
    }

    @Test
    void testCustomConfig() throws NoSuchFieldException, IllegalAccessException {
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