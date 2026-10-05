/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EntityConfigTest {
    private static final String KEY_ATTRIBUTES_FIELD = "keyAttributes";
    private static final String ATTRIBUTES_FIELD = "attributes";
    private static final String MAX_CARDINALITY_FIELD = "maxCardinality";

    private static Map<String, String> validKeyAttributes() {
        final Map<String, String> keyAttributes = new HashMap<>();
        keyAttributes.put("Type", "Service");
        keyAttributes.put("Name", "my-app");
        return keyAttributes;
    }

    private ObjectMapper objectMapper;
    private Validator validator;
    private ExpressionEvaluator expressionEvaluator;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        validator = Validation.byDefaultProvider()
                .configure()
                .messageInterpolator(new ParameterMessageInterpolator())
                .buildValidatorFactory()
                .getValidator();

        // Mirror the real evaluator's contract: a value carrying a ${...} field reference yields a
        // non-empty key list; a plain constant yields empty lists.
        expressionEvaluator = mock(ExpressionEvaluator.class);
        when(expressionEvaluator.extractDynamicKeysFromFormatExpression(anyString()))
                .thenAnswer(invocation -> {
                    final String value = invocation.getArgument(0);
                    return value != null && value.contains("${")
                            ? List.of(value) : Collections.<String>emptyList();
                });
        when(expressionEvaluator.extractDynamicExpressionsFromFormatExpression(anyString()))
                .thenReturn(Collections.emptyList());
    }

    @Test
    void GIVEN_new_entity_config_WHEN_get_key_attributes_called_SHOULD_return_empty_map() {
        final EntityConfig entityConfig = new EntityConfig();

        assertThat(entityConfig.getKeyAttributes(), notNullValue());
        assertThat(entityConfig.getKeyAttributes(), aMapWithSize(0));
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
    void GIVEN_entity_config_with_default_key_attributes_WHEN_validated_SHOULD_fail_NotEmpty_constraint() {
        final EntityConfig entityConfig = new EntityConfig();

        final Set<ConstraintViolation<EntityConfig>> violations = validator.validate(entityConfig);

        assertThat(violations, hasSize(1));
        final ConstraintViolation<EntityConfig> violation = violations.iterator().next();
        assertThat(violation.getPropertyPath().toString(), equalTo(KEY_ATTRIBUTES_FIELD));
    }

    @Test
    void GIVEN_null_attributes_WHEN_validated_SHOULD_fail_NotNull_constraint()
            throws NoSuchFieldException, IllegalAccessException {
        final EntityConfig entityConfig = new EntityConfig();
        final Map<String, String> keyAttributes = new HashMap<>();
        keyAttributes.put("Type", "Service");
        keyAttributes.put("Name", "my-app");
        ReflectivelySetField.setField(EntityConfig.class, entityConfig, KEY_ATTRIBUTES_FIELD, keyAttributes);
        ReflectivelySetField.setField(EntityConfig.class, entityConfig, ATTRIBUTES_FIELD, null);

        final Set<ConstraintViolation<EntityConfig>> violations = validator.validate(entityConfig);

        assertThat(violations, hasSize(1));
        assertThat(violations.iterator().next().getPropertyPath().toString(), equalTo(ATTRIBUTES_FIELD));
    }

    @Test
    void GIVEN_key_attributes_set_WHEN_put_on_returned_map_SHOULD_throw_unsupported_operation()
            throws NoSuchFieldException, IllegalAccessException {
        final EntityConfig entityConfig = new EntityConfig();
        final Map<String, String> keyAttributes = new HashMap<>();
        keyAttributes.put("Type", "RemoteService");
        ReflectivelySetField.setField(EntityConfig.class, entityConfig, KEY_ATTRIBUTES_FIELD, keyAttributes);

        assertThrows(UnsupportedOperationException.class,
                () -> entityConfig.getKeyAttributes().put("new", "value"));
    }

    @Test
    void GIVEN_attributes_set_WHEN_put_on_returned_map_SHOULD_throw_unsupported_operation()
            throws NoSuchFieldException, IllegalAccessException {
        final EntityConfig entityConfig = new EntityConfig();
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("key", "value");
        ReflectivelySetField.setField(EntityConfig.class, entityConfig, ATTRIBUTES_FIELD, attributes);

        assertThrows(UnsupportedOperationException.class,
                () -> entityConfig.getAttributes().put("new", "value"));
    }

    @Test
    void GIVEN_new_entity_config_WHEN_dynamic_getters_called_SHOULD_return_defaults() {
        final EntityConfig entityConfig = new EntityConfig();

        assertThat(entityConfig.isDynamic(expressionEvaluator), equalTo(false));
    }

    @Test
    void GIVEN_templated_key_attributes_in_yaml_WHEN_deserialized_SHOULD_be_dynamic() {
        final Map<String, String> keyAttributes = new HashMap<>();
        keyAttributes.put("Type", "Azure::Resource");
        keyAttributes.put("Identifier", "${resourceId}");
        final Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("key_attributes", keyAttributes);

        final EntityConfig entityConfig = objectMapper.convertValue(jsonMap, EntityConfig.class);

        assertThat(entityConfig.isDynamic(expressionEvaluator), equalTo(true));
    }

    @Test
    void GIVEN_no_templated_attributes_WHEN_is_dynamic_called_SHOULD_be_static() {
        final EntityConfig entityConfig = new EntityConfig();

        assertThat(entityConfig.isDynamic(expressionEvaluator), equalTo(false));
    }

    @Test
    void GIVEN_only_constant_key_attributes_WHEN_is_dynamic_called_SHOULD_be_static()
            throws NoSuchFieldException, IllegalAccessException {
        final EntityConfig entityConfig = new EntityConfig();
        final Map<String, String> keyAttributes = new HashMap<>();
        keyAttributes.put("Type", "Service");
        keyAttributes.put("Name", "my-app");
        ReflectivelySetField.setField(EntityConfig.class, entityConfig, KEY_ATTRIBUTES_FIELD, keyAttributes);

        assertThat(entityConfig.isDynamic(expressionEvaluator), equalTo(false));
    }

    @Test
    void GIVEN_templated_key_attribute_WHEN_is_dynamic_called_SHOULD_be_dynamic()
            throws NoSuchFieldException, IllegalAccessException {
        final EntityConfig entityConfig = new EntityConfig();
        final Map<String, String> keyAttributes = new HashMap<>();
        keyAttributes.put("Type", "Azure::Resource");
        keyAttributes.put("Identifier", "${resourceId}");
        ReflectivelySetField.setField(EntityConfig.class, entityConfig, KEY_ATTRIBUTES_FIELD, keyAttributes);

        assertThat(entityConfig.isDynamic(expressionEvaluator), equalTo(true));
    }

    @Test
    void GIVEN_new_entity_config_WHEN_get_max_cardinality_called_SHOULD_return_default() {
        final EntityConfig entityConfig = new EntityConfig();

        assertThat(entityConfig.getMaxCardinality(), equalTo(EntityConfig.DEFAULT_MAX_CARDINALITY));
    }

    @Test
    void GIVEN_max_cardinality_in_yaml_WHEN_deserialized_SHOULD_override_the_default() {
        final Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("key_attributes", validKeyAttributes());
        jsonMap.put("max_cardinality", 25);

        final EntityConfig entityConfig = objectMapper.convertValue(jsonMap, EntityConfig.class);

        assertThat(entityConfig.getMaxCardinality(), equalTo(25));
    }

    @Test
    void GIVEN_max_cardinality_below_one_WHEN_validated_SHOULD_fail_Min_constraint()
            throws NoSuchFieldException, IllegalAccessException {
        final EntityConfig entityConfig = new EntityConfig();
        ReflectivelySetField.setField(EntityConfig.class, entityConfig, KEY_ATTRIBUTES_FIELD, validKeyAttributes());
        // Zero would mean every event overflows into the entity-less fallback group, which is never a
        // useful configuration; the bound has to admit at least one entity.
        ReflectivelySetField.setField(EntityConfig.class, entityConfig, MAX_CARDINALITY_FIELD, 0);

        final Set<ConstraintViolation<EntityConfig>> violations = validator.validate(entityConfig);

        assertThat(violations, hasSize(1));
        assertThat(violations.iterator().next().getPropertyPath().toString(), equalTo(MAX_CARDINALITY_FIELD));
    }

    @Test
    void GIVEN_templated_attribute_only_WHEN_is_dynamic_called_SHOULD_be_dynamic()
            throws NoSuchFieldException, IllegalAccessException {
        final EntityConfig entityConfig = new EntityConfig();
        final Map<String, String> keyAttributes = new HashMap<>();
        keyAttributes.put("Type", "Service");
        keyAttributes.put("Name", "my-app");
        ReflectivelySetField.setField(EntityConfig.class, entityConfig, KEY_ATTRIBUTES_FIELD, keyAttributes);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("AWS.ServiceNameSource", "${source}");
        ReflectivelySetField.setField(EntityConfig.class, entityConfig, ATTRIBUTES_FIELD, attributes);

        assertThat(entityConfig.isDynamic(expressionEvaluator), equalTo(true));
    }
}
