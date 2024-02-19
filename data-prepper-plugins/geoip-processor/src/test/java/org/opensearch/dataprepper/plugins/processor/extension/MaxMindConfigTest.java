/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.extension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.net.URISyntaxException;
import java.time.Duration;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class MaxMindConfigTest {
    private MaxMindConfig maxMindConfig;

    @BeforeEach
    void setup() {
        maxMindConfig = new MaxMindConfig();
    }

    @Test
    void testDefaultConfig() {
        assertThat(maxMindConfig.getDatabasePaths().size(), equalTo(0));
        assertThat(maxMindConfig.getDatabaseRefreshInterval(), equalTo(Duration.ofDays(7)));
        assertThat(maxMindConfig.getCacheSize(), equalTo(4096));
        assertThat(maxMindConfig.getAwsAuthenticationOptionsConfig(), equalTo(null));
    }

    @Test
    void testCustomConfig() throws NoSuchFieldException, IllegalAccessException {
        ReflectivelySetField.setField(MaxMindConfig.class, maxMindConfig, "databaseRefreshInterval", Duration.ofDays(10));
        ReflectivelySetField.setField(MaxMindConfig.class, maxMindConfig, "cacheSize", 2048);
        ReflectivelySetField.setField(MaxMindConfig.class, maxMindConfig, "databasePaths", List.of("path1", "path2", "path3"));

        final AwsAuthenticationOptionsConfig awsAuthenticationOptionsConfig = new AwsAuthenticationOptionsConfig();
        ReflectivelySetField.setField(MaxMindConfig.class, maxMindConfig, "awsAuthenticationOptionsConfig", awsAuthenticationOptionsConfig);

        assertThat(maxMindConfig.getDatabaseRefreshInterval(), equalTo(Duration.ofDays(10)));
        assertThat(maxMindConfig.getCacheSize(), equalTo(2048));
        assertThat(maxMindConfig.getDatabasePaths().size(), equalTo(3));
        assertThat(maxMindConfig.getAwsAuthenticationOptionsConfig(), equalTo(awsAuthenticationOptionsConfig));
    }

    @ParameterizedTest
    @CsvSource({
            "https://example.com/, false, true",
            "http://example.com/, false, false",
            "https://example.com/, true, true",
            "http://example.com/, true, true"})
    void testSecureEndpoint(final String databasePath, final boolean insecure, final boolean result)
            throws NoSuchFieldException, IllegalAccessException, URISyntaxException {
        ReflectivelySetField.setField(MaxMindConfig.class, maxMindConfig, "databasePaths", List.of(databasePath));
        ReflectivelySetField.setField(MaxMindConfig.class, maxMindConfig, "insecure", insecure);

        assertThat(maxMindConfig.getDatabasePaths().size(), equalTo(1));
        assertThat(maxMindConfig.isHttpsEndpointOrInsecure(), equalTo(result));
    }
}