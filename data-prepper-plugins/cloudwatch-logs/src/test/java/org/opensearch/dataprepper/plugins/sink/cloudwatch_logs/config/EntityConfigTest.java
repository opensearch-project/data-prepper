/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

class EntityConfigTest {
    private static final String KEY_ATTRIBUTES_FIELD = "keyAttributes";
    private static final String ATTRIBUTES_FIELD = "attributes";

    private ObjectMapper objectMapper;
    private Validator validator;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        validator = Validation.byDefaultProvider()
                .configure()
                .messageInterpolator(new ParameterMessageInterpolator())
                .buildValidatorFactory()
                .getValidator();
    }

    @Test
    void GIVEN_new_entity_config_WHEN_get_key_attributes_called_SHOULD_return_null() {
        final EntityConfig entityConfig = new EntityConfig();

        assertThat(entityConfig.getKeyAttributes(), nullValue());
    }

    @Test
    void GIVEN_new_entity_config_WHEN_get_attributes_called_SHOULD_return_empty_map() {
        final EntityConfig entityConfig = new EntityConfig();

        assertThat(entityConfig.getAttributes(), notNullValue());
        assertThat(entityConfig.getAttributes(), aMapWithSize(0));
    }

    @Test
    void GIVEN_key_attributes_set_WHEN_get_key_attributes_called_SHOULD_return_configured_value()
            throws NoSuchFieldException, IllegalAccessException {
        final EntityConfig entityConfig = new EntityConfig();
        final Map<String, String> keyAttributes = new HashMap<>();
        keyAttributes.put("Type", "RemoteService");
        keyAttributes.put("Name", "okta_auth0");

        ReflectivelySetField.setField(EntityConfig.class, entityConfig, KEY_ATTRIBUTES_FIELD, keyAttributes);

        assertThat(entityConfig.getKeyAttributes(), equalTo(keyAttributes));
    }

    @Test
    void GIVEN_attributes_set_WHEN_get_attributes_called_SHOULD_return_configured_value()
            throws NoSuchFieldException, IllegalAccessException {
        final EntityConfig entityConfig = new EntityConfig();
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("AWS.ServiceNameSource", "UserConfiguration");

        ReflectivelySetField.setField(EntityConfig.class, entityConfig, ATTRIBUTES_FIELD, attributes);

        assertThat(entityConfig.getAttributes(), equalTo(attributes));
    }

    @Test
    void GIVEN_entity_config_with_key_attributes_WHEN_deserialized_from_map_SHOULD_populate_fields() {
        final Map<String, String> keyAttributes = new HashMap<>();
        keyAttributes.put("Type", "RemoteService");
        keyAttributes.put("Name", "okta_auth0");
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("AWS.ServiceNameSource", "UserConfiguration");
        final Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("key_attributes", keyAttributes);
        jsonMap.put("attributes", attributes);

        final EntityConfig entityConfig = objectMapper.convertValue(jsonMap, EntityConfig.class);

        assertThat(entityConfig.getKeyAttributes(), equalTo(keyAttributes));
        assertThat(entityConfig.getAttributes(), equalTo(attributes));
    }

    @Test
    void GIVEN_entity_config_with_only_key_attributes_in_yaml_WHEN_deserialized_SHOULD_default_attributes_to_empty_map() {
        final Map<String, String> keyAttributes = new HashMap<>();
        keyAttributes.put("Type", "RemoteService");
        keyAttributes.put("Name", "okta_auth0");
        final Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("key_attributes", keyAttributes);

        final EntityConfig entityConfig = objectMapper.convertValue(jsonMap, EntityConfig.class);

        assertThat(entityConfig.getKeyAttributes(), equalTo(keyAttributes));
        assertThat(entityConfig.getAttributes(), notNullValue());
        assertThat(entityConfig.getAttributes(), aMapWithSize(0));
    }

    @Test
    void GIVEN_entity_config_with_empty_key_attributes_WHEN_validated_SHOULD_fail_NotEmpty_constraint()
            throws NoSuchFieldException, IllegalAccessException {
        final EntityConfig entityConfig = new EntityConfig();
        ReflectivelySetField.setField(EntityConfig.class, entityConfig, KEY_ATTRIBUTES_FIELD, Collections.emptyMap());

        final Set<ConstraintViolation<EntityConfig>> violations = validator.validate(entityConfig);

        assertThat(violations, hasSize(1));
        final ConstraintViolation<EntityConfig> violation = violations.iterator().next();
        assertThat(violation.getPropertyPath().toString(), equalTo(KEY_ATTRIBUTES_FIELD));
    }

    @Test
    void GIVEN_entity_config_with_null_key_attributes_WHEN_validated_SHOULD_fail_NotEmpty_constraint() {
        final EntityConfig entityConfig = new EntityConfig();

        final Set<ConstraintViolation<EntityConfig>> violations = validator.validate(entityConfig);

        assertThat(violations, hasSize(1));
        final ConstraintViolation<EntityConfig> violation = violations.iterator().next();
        assertThat(violation.getPropertyPath().toString(), equalTo(KEY_ATTRIBUTES_FIELD));
    }
}
