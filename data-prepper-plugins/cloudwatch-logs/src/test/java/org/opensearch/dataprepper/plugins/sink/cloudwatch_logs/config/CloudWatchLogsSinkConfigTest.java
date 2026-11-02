/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import jakarta.validation.ConstraintValidatorContext;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

class CloudWatchLogsSinkConfigTest {
    private CloudWatchLogsSinkConfig cloudWatchLogsSinkConfig;
    private AwsConfig awsConfig;
    private ThresholdConfig thresholdConfig;
    private final String LOG_GROUP = "testLogGroup";
    private final String LOG_STREAM = "testLogStream";
    
    @Mock
    private ConstraintValidatorContext context;
    
    @Mock
    private ConstraintValidatorContext.ConstraintViolationBuilder violationBuilder;
    
    private ValidCustomHeaders.CustomHeadersValidator validator;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        cloudWatchLogsSinkConfig = new CloudWatchLogsSinkConfig();
        awsConfig = new AwsConfig();
        thresholdConfig = new ThresholdConfig();
        validator = new ValidCustomHeaders.CustomHeadersValidator();
        
        when(context.buildConstraintViolationWithTemplate(anyString())).thenReturn(violationBuilder);
        when(violationBuilder.addConstraintViolation()).thenReturn(context);
    }

    @Test
    void GIVEN_new_sink_config_WHEN_accessed_SHOULD_return_defaults_and_maintain_backward_compatibility()
            throws NoSuchFieldException, IllegalAccessException {
        final CloudWatchLogsSinkConfig newSinkConfig = new CloudWatchLogsSinkConfig();
        assertThat(newSinkConfig.getAwsConfig(), nullValue());
        assertThat(newSinkConfig.getThresholdConfig(), notNullValue());
        assertThat(newSinkConfig.getEndpoint(), nullValue());
        assertThat(newSinkConfig.getCreateLogGroup(), equalTo(false));
        assertThat(newSinkConfig.getCreateLogStream(), equalTo(false));
        assertThat(newSinkConfig.getEntityConfig(), nullValue());

        ReflectivelySetField.setField(cloudWatchLogsSinkConfig.getClass(), cloudWatchLogsSinkConfig, "logGroup", LOG_GROUP);
        ReflectivelySetField.setField(cloudWatchLogsSinkConfig.getClass(), cloudWatchLogsSinkConfig, "logStream", LOG_STREAM);
        ReflectivelySetField.setField(cloudWatchLogsSinkConfig.getClass(), cloudWatchLogsSinkConfig, "thresholdConfig", thresholdConfig);
        ReflectivelySetField.setField(cloudWatchLogsSinkConfig.getClass(), cloudWatchLogsSinkConfig, "awsConfig", awsConfig);

        // Verify that existing functionality works without header overrides
        assertThat(cloudWatchLogsSinkConfig.getLogGroup(), equalTo(LOG_GROUP));
        assertThat(cloudWatchLogsSinkConfig.getLogStream(), equalTo(LOG_STREAM));
        assertThat(cloudWatchLogsSinkConfig.getAwsConfig(), equalTo(awsConfig));
        assertThat(cloudWatchLogsSinkConfig.getThresholdConfig(), equalTo(thresholdConfig));
        assertThat(cloudWatchLogsSinkConfig.getHeaderOverrides(), notNullValue());
        assertThat(cloudWatchLogsSinkConfig.getHeaderOverrides().isEmpty(), equalTo(true));
        assertThat(cloudWatchLogsSinkConfig.getWorkers(), equalTo(10));
        assertThat(cloudWatchLogsSinkConfig.getMaxRetries(), equalTo(5));
    }

    @ParameterizedTest
    @NullAndEmptySource
    void GIVEN_null_or_empty_custom_headers_WHEN_validated_THEN_returns_true(final Map<String, String> headers) {
        final boolean result = validator.isValid(headers, context);
        
        assertThat(result, equalTo(true));
        verify(context, never()).disableDefaultConstraintViolation();
    }

    @Test
    void GIVEN_null_header_name_WHEN_validated_THEN_returns_false() {
        Map<String, String> headersWithNullKey = new HashMap<>();
        headersWithNullKey.put(null, "some-value");
        
        boolean result = validator.isValid(headersWithNullKey, context);
        
        assertThat(result, equalTo(false));
        verify(context).disableDefaultConstraintViolation();
        verify(context).buildConstraintViolationWithTemplate("Header name cannot be null");
    }

    @Test
    void GIVEN_null_header_value_WHEN_validated_THEN_returns_false() {
        Map<String, String> headersWithNullValue = new HashMap<>();
        headersWithNullValue.put("X-Test-Header", null);
        
        boolean result = validator.isValid(headersWithNullValue, context);
        
        assertThat(result, equalTo(false));
        verify(context).disableDefaultConstraintViolation();
        verify(context).buildConstraintViolationWithTemplate("Header 'X-Test-Header' has an empty value");
    }

    @Test
    void GIVEN_empty_header_value_WHEN_validated_THEN_returns_false() {
        Map<String, String> headersWithEmptyValue = new HashMap<>();
        headersWithEmptyValue.put("X-Test-Header", "   ");
        
        boolean result = validator.isValid(headersWithEmptyValue, context);
        
        assertThat(result, equalTo(false));
        verify(context).disableDefaultConstraintViolation();
        verify(context).buildConstraintViolationWithTemplate("Header 'X-Test-Header' has an empty value");
    }

    @ParameterizedTest
    @ValueSource(strings = {"Invalid Header Name", "Header@Name"})
    void GIVEN_invalid_header_name_WHEN_validated_THEN_returns_false(final String headerName) {
        final Map<String, String> headersWithInvalidName = new HashMap<>();
        headersWithInvalidName.put(headerName, "valid-value");

        final boolean result = validator.isValid(headersWithInvalidName, context);

        assertThat(result, equalTo(false));
        verify(context).disableDefaultConstraintViolation();
        verify(context).buildConstraintViolationWithTemplate("Invalid header name '" + headerName + "'");
    }

    @Test
    void GIVEN_valid_header_names_with_allowed_special_chars_WHEN_validated_THEN_returns_true() {
        Map<String, String> validHeaders = new HashMap<>();
        validHeaders.put("X-Custom-Header", "value1");
        validHeaders.put("Content_Type", "value2");
        validHeaders.put("Accept.Encoding", "value3");
        validHeaders.put("Cache~Control", "value4");
        
        boolean result = validator.isValid(validHeaders, context);
        
        assertThat(result, equalTo(true));
        verify(context, never()).disableDefaultConstraintViolation();
    }

    @ParameterizedTest
    @CsvSource({"10, false", "11, true"})
    void GIVEN_header_overrides_WHEN_sink_config_validated_THEN_more_than_10_rejected(final int headerCount,
                                                                                     final boolean expectViolation)
            throws NoSuchFieldException, IllegalAccessException {
        final Map<String, String> headers = new HashMap<>();
        for (int i = 1; i <= headerCount; i++) {
            headers.put("Header-" + i, "value-" + i);
        }
        ReflectivelySetField.setField(cloudWatchLogsSinkConfig.getClass(), cloudWatchLogsSinkConfig, "headerOverrides", headers);

        final Set<ConstraintViolation<CloudWatchLogsSinkConfig>> violations = beanValidator().validate(cloudWatchLogsSinkConfig);

        assertThat(violations.stream()
                .anyMatch(v -> v.getPropertyPath().toString().equals("headerOverrides")), equalTo(expectViolation));
    }

    @Test
    void GIVEN_invalid_entity_config_WHEN_sink_config_validated_SHOULD_propagate_validation_error()
            throws NoSuchFieldException, IllegalAccessException {
        final EntityConfig invalidEntityConfig = new EntityConfig();
        ReflectivelySetField.setField(cloudWatchLogsSinkConfig.getClass(), cloudWatchLogsSinkConfig, "logGroup", LOG_GROUP);
        ReflectivelySetField.setField(cloudWatchLogsSinkConfig.getClass(), cloudWatchLogsSinkConfig, "logStream", LOG_STREAM);
        ReflectivelySetField.setField(cloudWatchLogsSinkConfig.getClass(), cloudWatchLogsSinkConfig, "entityConfig", invalidEntityConfig);

        final Set<ConstraintViolation<CloudWatchLogsSinkConfig>> violations = beanValidator().validate(cloudWatchLogsSinkConfig);

        assertThat(violations.stream()
                .anyMatch(v -> v.getPropertyPath().toString().equals("entityConfig.keyAttributes")), equalTo(true));
    }

    private static Validator beanValidator() {
        return Validation.byDefaultProvider()
                .configure()
                .messageInterpolator(new ParameterMessageInterpolator())
                .buildValidatorFactory()
                .getValidator();
    }
}
