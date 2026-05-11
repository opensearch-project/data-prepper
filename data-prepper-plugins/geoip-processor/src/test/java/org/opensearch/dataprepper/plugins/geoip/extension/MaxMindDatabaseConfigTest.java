/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.geoip.extension;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.dataprepper.plugins.geoip.extension.MaxMindDatabaseConfig.DEFAULT_ASN_ENDPOINT;
import static org.opensearch.dataprepper.plugins.geoip.extension.MaxMindDatabaseConfig.DEFAULT_CITY_ENDPOINT;
import static org.opensearch.dataprepper.plugins.geoip.extension.MaxMindDatabaseConfig.DEFAULT_COUNTRY_ENDPOINT;
import static org.opensearch.dataprepper.plugins.geoip.extension.MaxMindDatabaseConfig.GEOIP2_ENTERPRISE;
import static org.opensearch.dataprepper.plugins.geoip.extension.MaxMindDatabaseConfig.GEOLITE2_ASN;
import static org.opensearch.dataprepper.plugins.geoip.extension.MaxMindDatabaseConfig.GEOLITE2_CITY;
import static org.opensearch.dataprepper.plugins.geoip.extension.MaxMindDatabaseConfig.GEOLITE2_COUNTRY;

class MaxMindDatabaseConfigTest {
    private MaxMindDatabaseConfig maxMindDatabaseConfig;
    private Validator validator;

    @BeforeEach
    void setup() {
        maxMindDatabaseConfig = new MaxMindDatabaseConfig();
        validator = Validation.byDefaultProvider()
                .configure()
                .messageInterpolator(new ParameterMessageInterpolator())
                .buildValidatorFactory()
                .getValidator();
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

    @Test
    void validate_should_include_path_specific_message_when_path_does_not_exist(@TempDir final Path tempDirectory)
            throws NoSuchFieldException, IllegalAccessException {
        final Path missingDatabase = tempDirectory.resolve("geoip.mmdb");
        ReflectivelySetField.setField(
                MaxMindDatabaseConfig.class, maxMindDatabaseConfig, "asnDatabase", missingDatabase.toString());

        assertThat(getValidationMessages(), containsInAnyOrder("Path does not exist: " + missingDatabase));
    }

    @Test
    void validate_should_include_path_specific_message_when_directory_is_configured(@TempDir final Path tempDirectory)
            throws NoSuchFieldException, IllegalAccessException {
        ReflectivelySetField.setField(
                MaxMindDatabaseConfig.class, maxMindDatabaseConfig, "asnDatabase", tempDirectory.toString());

        assertThat(getValidationMessages(),
                containsInAnyOrder("Directory provided, but a file is required: " + tempDirectory));
    }

    @Test
    void validate_should_include_path_specific_message_when_http_endpoint_is_not_supported()
            throws NoSuchFieldException, IllegalAccessException {
        ReflectivelySetField.setField(
                MaxMindDatabaseConfig.class, maxMindDatabaseConfig, "asnDatabase", "https://example.com/");

        assertThat(getValidationMessages(), containsInAnyOrder(
                "HTTP endpoint must be a MaxMind download URL or manifest endpoint: https://example.com/"));
    }

    @Test
    void validate_should_include_path_specific_messages_when_source_types_are_mixed(@TempDir final Path tempDirectory)
            throws NoSuchFieldException, IllegalAccessException, IOException {
        final Path cityDatabase = Files.createFile(tempDirectory.resolve("GeoLite2-City.mmdb"));
        final String s3DatabasePath = "s3://geoip/GeoLite2-ASN.mmdb";
        ReflectivelySetField.setField(
                MaxMindDatabaseConfig.class, maxMindDatabaseConfig, "cityDatabase", cityDatabase.toString());
        ReflectivelySetField.setField(
                MaxMindDatabaseConfig.class, maxMindDatabaseConfig, "asnDatabase", s3DatabasePath);

        assertThat(getValidationMessages(), containsInAnyOrder(
                "Mixed database path source types are not supported. Found local file path: " + cityDatabase,
                "Mixed database path source types are not supported. Found S3 URI: " + s3DatabasePath));
    }

    private Set<String> getValidationMessages() {
        return validator.validate(maxMindDatabaseConfig).stream()
                .map(ConstraintViolation::getMessage)
                .collect(Collectors.toSet());
    }

}
