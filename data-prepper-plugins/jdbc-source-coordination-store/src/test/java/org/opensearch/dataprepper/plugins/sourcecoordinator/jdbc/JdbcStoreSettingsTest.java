/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sourcecoordinator.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

class JdbcStoreSettingsTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    private ValidatorFactory validatorFactory;
    private Validator validator;

    @BeforeEach
    void setUp() {
        validatorFactory = Validation.byDefaultProvider()
                .configure()
                .messageInterpolator(new ParameterMessageInterpolator())
                .buildValidatorFactory();
        validator = validatorFactory.getValidator();
    }

    @AfterEach
    void tearDown() {
        validatorFactory.close();
    }

    private static Map<String, Object> requiredSettings() {
        final Map<String, Object> settings = new HashMap<>();
        settings.put("url", "jdbc:postgresql://localhost/db");
        settings.put("username", "user");
        settings.put("password", "pass");
        return settings;
    }

    @Test
    void deserialization_with_required_fields_applies_defaults() {
        final JdbcStoreSettings settings = OBJECT_MAPPER.convertValue(requiredSettings(), JdbcStoreSettings.class);

        assertThat(settings.getUrl(), equalTo("jdbc:postgresql://localhost/db"));
        assertThat(settings.getUsername(), equalTo("user"));
        assertThat(settings.getPassword(), equalTo("pass"));
        assertThat(settings.getTableName(), equalTo("source_coordination"));
        assertThat(settings.skipTableCreation(), is(false));
        assertThat(settings.getMaxPoolSize(), equalTo(5));
        assertThat(settings.getTtl(), is(nullValue()));
        assertThat(settings.getConnectionProperties(), is(nullValue()));
        assertThat(validator.validate(settings), is(empty()));
    }

    @Test
    void deserialization_with_all_fields() {
        final Map<String, Object> settingsMap = requiredSettings();
        settingsMap.put("table_name", "custom_table");
        settingsMap.put("skip_table_creation", true);
        settingsMap.put("max_pool_size", 10);
        settingsMap.put("ttl", "PT24H");
        settingsMap.put("connection_properties", Map.of("ssl", "true"));

        final JdbcStoreSettings settings = OBJECT_MAPPER.convertValue(settingsMap, JdbcStoreSettings.class);

        assertThat(settings.getTableName(), equalTo("custom_table"));
        assertThat(settings.skipTableCreation(), is(true));
        assertThat(settings.getMaxPoolSize(), equalTo(10));
        assertThat(settings.getTtl(), equalTo(Duration.ofHours(24)));
        assertThat(settings.getConnectionProperties(), equalTo(Map.of("ssl", "true")));
        assertThat(validator.validate(settings), is(empty()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"url", "username", "password"})
    void validation_fails_when_required_field_is_missing(final String requiredField) {
        final Map<String, Object> settingsMap = requiredSettings();
        settingsMap.remove(requiredField);

        final JdbcStoreSettings settings = OBJECT_MAPPER.convertValue(settingsMap, JdbcStoreSettings.class);
        final Set<ConstraintViolation<JdbcStoreSettings>> violations = validator.validate(settings);

        assertThat(violations, is(not(empty())));
        assertThat(violations.iterator().next().getMessage(),
                equalTo(requiredField + " is required for JDBC store settings"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"url", "username", "password"})
    void validation_fails_when_required_field_is_empty(final String requiredField) {
        final Map<String, Object> settingsMap = requiredSettings();
        settingsMap.put(requiredField, "");

        final JdbcStoreSettings settings = OBJECT_MAPPER.convertValue(settingsMap, JdbcStoreSettings.class);

        assertThat(validator.validate(settings), is(not(empty())));
    }

    @Test
    void validation_fails_when_max_pool_size_is_below_one() {
        final Map<String, Object> settingsMap = requiredSettings();
        settingsMap.put("max_pool_size", 0);

        final JdbcStoreSettings settings = OBJECT_MAPPER.convertValue(settingsMap, JdbcStoreSettings.class);

        assertThat(validator.validate(settings), is(not(empty())));
    }

    @Test
    void validation_fails_when_table_name_is_null() {
        final Map<String, Object> settingsMap = requiredSettings();
        settingsMap.put("table_name", null);

        final JdbcStoreSettings settings = OBJECT_MAPPER.convertValue(settingsMap, JdbcStoreSettings.class);

        assertThat(validator.validate(settings), is(not(empty())));
    }

    @ParameterizedTest
    @ValueSource(strings = {"source_coordination", "my_schema.my_table", "_private", "Table123"})
    void validation_passes_for_valid_table_names(final String tableName) {
        final Map<String, Object> settingsMap = requiredSettings();
        settingsMap.put("table_name", tableName);

        final JdbcStoreSettings settings = OBJECT_MAPPER.convertValue(settingsMap, JdbcStoreSettings.class);

        assertThat(validator.validate(settings), is(empty()));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "foo;DROP TABLE bar",
            "foo bar",
            "foo-bar",
            "1foo",
            "foo\"bar",
            "foo'bar",
            "schema.table.extra",
            ".foo",
            "foo."
    })
    void validation_fails_for_unsafe_table_names(final String tableName) {
        final Map<String, Object> settingsMap = requiredSettings();
        settingsMap.put("table_name", tableName);

        final JdbcStoreSettings settings = OBJECT_MAPPER.convertValue(settingsMap, JdbcStoreSettings.class);

        assertThat(validator.validate(settings), is(not(empty())));
    }
}
