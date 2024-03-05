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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.dataprepper.plugins.processor.extension.MaxMindDatabaseConfig.DEFAULT_ASN_ENDPOINT;
import static org.opensearch.dataprepper.plugins.processor.extension.MaxMindDatabaseConfig.DEFAULT_CITY_ENDPOINT;
import static org.opensearch.dataprepper.plugins.processor.extension.MaxMindDatabaseConfig.DEFAULT_COUNTRY_ENDPOINT;
import static org.opensearch.dataprepper.plugins.processor.extension.MaxMindDatabaseConfig.GEOIP2_ENTERPRISE;
import static org.opensearch.dataprepper.plugins.processor.extension.MaxMindDatabaseConfig.GEOLITE2_ASN;
import static org.opensearch.dataprepper.plugins.processor.extension.MaxMindDatabaseConfig.GEOLITE2_CITY;
import static org.opensearch.dataprepper.plugins.processor.extension.MaxMindDatabaseConfig.GEOLITE2_COUNTRY;

class MaxMindDatabaseConfigTest {
    private MaxMindDatabaseConfig maxMindDatabaseConfig;
    @BeforeEach
    void setup() {
        maxMindDatabaseConfig = new MaxMindDatabaseConfig();
    }

    @Test
    void test_default_values() {
        assertThat(maxMindDatabaseConfig.getDatabasePaths().get(GEOLITE2_CITY), equalTo(DEFAULT_CITY_ENDPOINT));
        assertThat(maxMindDatabaseConfig.getDatabasePaths().get(GEOLITE2_COUNTRY), equalTo(DEFAULT_COUNTRY_ENDPOINT));
        assertThat(maxMindDatabaseConfig.getDatabasePaths().get(GEOLITE2_ASN), equalTo(DEFAULT_ASN_ENDPOINT));
        assertThat(maxMindDatabaseConfig.getDatabasePaths().get(GEOIP2_ENTERPRISE), equalTo(null));
        assertThat(maxMindDatabaseConfig.getDatabasePaths().size(), equalTo(3));
    }

    @Test
    void test_getDatabasePaths_should_not_use_default_endpoints_if_enterprise_database_is_configured() throws NoSuchFieldException, IllegalAccessException {
        ReflectivelySetField.setField(MaxMindDatabaseConfig.class, maxMindDatabaseConfig, "enterpriseDatabase", "enterprise_database_path");

        assertThat(maxMindDatabaseConfig.getDatabasePaths().size(), equalTo(1));
        assertThat(maxMindDatabaseConfig.getDatabasePaths().get(GEOLITE2_CITY), equalTo(null));
        assertThat(maxMindDatabaseConfig.getDatabasePaths().get(GEOLITE2_COUNTRY), equalTo(null));
        assertThat(maxMindDatabaseConfig.getDatabasePaths().get(GEOLITE2_ASN), equalTo(null));
        assertThat(maxMindDatabaseConfig.getDatabasePaths().get(GEOIP2_ENTERPRISE), equalTo("enterprise_database_path"));
    }

    @Test
    void test_getDatabasePaths_should_use_only_configured_endpoints() throws NoSuchFieldException, IllegalAccessException {
        ReflectivelySetField.setField(MaxMindDatabaseConfig.class, maxMindDatabaseConfig, "cityDatabase", "city_database_path");
        ReflectivelySetField.setField(MaxMindDatabaseConfig.class, maxMindDatabaseConfig, "countryDatabase", "country_database_path");

        assertThat(maxMindDatabaseConfig.getDatabasePaths().size(), equalTo(2));
        assertThat(maxMindDatabaseConfig.getDatabasePaths().get(GEOLITE2_CITY), equalTo("city_database_path"));
        assertThat(maxMindDatabaseConfig.getDatabasePaths().get(GEOLITE2_COUNTRY), equalTo("country_database_path"));
        assertThat(maxMindDatabaseConfig.getDatabasePaths().get(GEOLITE2_ASN), equalTo(null));
        assertThat(maxMindDatabaseConfig.getDatabasePaths().get(GEOIP2_ENTERPRISE), equalTo(null));
    }

    @Test
    void test_custom_endpoints() throws NoSuchFieldException, IllegalAccessException {
        ReflectivelySetField.setField(MaxMindDatabaseConfig.class, maxMindDatabaseConfig, "cityDatabase", "city_database_path");
        ReflectivelySetField.setField(MaxMindDatabaseConfig.class, maxMindDatabaseConfig, "countryDatabase", "country_database_path");
        ReflectivelySetField.setField(MaxMindDatabaseConfig.class, maxMindDatabaseConfig, "asnDatabase", "asn_database_path");
        ReflectivelySetField.setField(MaxMindDatabaseConfig.class, maxMindDatabaseConfig, "enterpriseDatabase", "enterprise_database_path");

        assertThat(maxMindDatabaseConfig.getDatabasePaths().size(), equalTo(4));
        assertThat(maxMindDatabaseConfig.getDatabasePaths().get(GEOLITE2_CITY), equalTo("city_database_path"));
        assertThat(maxMindDatabaseConfig.getDatabasePaths().get(GEOLITE2_COUNTRY), equalTo("country_database_path"));
        assertThat(maxMindDatabaseConfig.getDatabasePaths().get(GEOLITE2_ASN), equalTo("asn_database_path"));
        assertThat(maxMindDatabaseConfig.getDatabasePaths().get(GEOIP2_ENTERPRISE), equalTo("enterprise_database_path"));
    }

    @Test
    void test_invalid_config_when_geolite2_and_enterprise_databases_are_configured() throws NoSuchFieldException, IllegalAccessException {
        ReflectivelySetField.setField(MaxMindDatabaseConfig.class, maxMindDatabaseConfig, "asnDatabase", "asn_database_path");
        ReflectivelySetField.setField(MaxMindDatabaseConfig.class, maxMindDatabaseConfig, "enterpriseDatabase", "enterprise_database_path");

        assertThat(maxMindDatabaseConfig.isDatabasesValid(), equalTo(false));
    }

    @ParameterizedTest
    @CsvSource({"https://download.maxmind.com/, true",
            "https://example.com/, false",
            "s3://geoip/data, true",
            "https://geoip.maps.opensearch.org/v1/mmdb/geolite2-city/manifest.json, true",
            "https://geoip.maps.opensearch.org/v1/mmdb/geolite2/input.json, false"})
    void test_isPathsValid(final String path, final boolean result) throws NoSuchFieldException, IllegalAccessException {
        ReflectivelySetField.setField(MaxMindDatabaseConfig.class, maxMindDatabaseConfig, "asnDatabase", path);

        assertThat(maxMindDatabaseConfig.isPathsValid(), equalTo(result));
    }

}