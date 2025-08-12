/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;

import jakarta.validation.ConstraintValidatorContext;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

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
    void GIVEN_new_sink_config_WHEN_get_aws_config_called_SHOULD_return_null() {
        assertThat(new CloudWatchLogsSinkConfig().getAwsConfig(), equalTo(null));
    }

    @Test
    void GIVEN_new_sink_config_WHEN_get_threshold_config_called_SHOULD_return_null() {
        assertThat(new CloudWatchLogsSinkConfig().getThresholdConfig(), notNullValue());
    }

    @Test
    void GIVEN_new_sink_config_WHEN_get_log_group_called_SHOULD_return_null() {
        assertThat(new CloudWatchLogsSinkConfig().getLogGroup(), equalTo(null));
    }

    @Test
    void GIVEN_new_sink_config_WHEN_get_num_threads_called_SHOULD_return_default_value() {
        assertThat(new CloudWatchLogsSinkConfig().getWorkers(), equalTo(CloudWatchLogsSinkConfig.DEFAULT_NUM_WORKERS));
    }

    @Test
    void GIVEN_new_sink_config_WHEN_get_log_stream_called_SHOULD_return_null() {
        assertThat(new CloudWatchLogsSinkConfig().getLogStream(), equalTo(null));
    }

    @Test
    void GIVEN_num_threads_configured_SHOULD_return_the_configured_value() throws NoSuchFieldException, IllegalAccessException {
        int testValue = (new Random()).nextInt();
        ReflectivelySetField.setField(cloudWatchLogsSinkConfig.getClass(), cloudWatchLogsSinkConfig, "workers", testValue);
        assertThat(cloudWatchLogsSinkConfig.getWorkers(), equalTo(testValue));
    }

    @Test
    void GIVEN_empty_sink_config_WHEN_deserialized_from_json_SHOULD_return_valid_log_group_and_log_stream() throws NoSuchFieldException, IllegalAccessException {
        ReflectivelySetField.setField(cloudWatchLogsSinkConfig.getClass(), cloudWatchLogsSinkConfig, "logGroup", LOG_GROUP);
        ReflectivelySetField.setField(cloudWatchLogsSinkConfig.getClass(), cloudWatchLogsSinkConfig, "logStream", LOG_STREAM);

        assertThat(cloudWatchLogsSinkConfig.getLogGroup(), equalTo(LOG_GROUP));
        assertThat(cloudWatchLogsSinkConfig.getLogStream(), equalTo(LOG_STREAM));
    }

    @Test
    void GIVEN_empty_sink_config_WHEN_deserialized_from_json_SHOULD_return_valid_threshold_config_and_aws_config() throws NoSuchFieldException, IllegalAccessException {
        ReflectivelySetField.setField(cloudWatchLogsSinkConfig.getClass(), cloudWatchLogsSinkConfig, "thresholdConfig", thresholdConfig);
        ReflectivelySetField.setField(cloudWatchLogsSinkConfig.getClass(), cloudWatchLogsSinkConfig, "awsConfig", awsConfig);

        assertThat(cloudWatchLogsSinkConfig.getAwsConfig(), equalTo(awsConfig));
        assertThat(cloudWatchLogsSinkConfig.getThresholdConfig(), equalTo(thresholdConfig));
    }

    @Test
    void GIVEN_new_sink_config_WHEN_get_header_overrides_called_SHOULD_return_empty_map() {
        assertThat(new CloudWatchLogsSinkConfig().getHeaderOverrides(), notNullValue());
        assertThat(new CloudWatchLogsSinkConfig().getHeaderOverrides().isEmpty(), equalTo(true));
    }

    @Test
    void GIVEN_header_overrides_set_WHEN_get_header_overrides_called_SHOULD_return_configured_value() throws NoSuchFieldException, IllegalAccessException {
        Map<String, String> headers = new HashMap<>();
        headers.put("X-Custom-Header", "custom-value");
        headers.put("X-Request-ID", "request-123");

        ReflectivelySetField.setField(cloudWatchLogsSinkConfig.getClass(), cloudWatchLogsSinkConfig, "headerOverrides", headers);

        assertThat(cloudWatchLogsSinkConfig.getHeaderOverrides(), equalTo(headers));
    }

    @Test
    void GIVEN_empty_header_overrides_WHEN_get_header_overrides_called_SHOULD_return_empty_headers() throws NoSuchFieldException, IllegalAccessException {
        Map<String, String> emptyHeaders = new HashMap<>();
        ReflectivelySetField.setField(cloudWatchLogsSinkConfig.getClass(), cloudWatchLogsSinkConfig, "headerOverrides", emptyHeaders);

        assertThat(cloudWatchLogsSinkConfig.getHeaderOverrides(), equalTo(emptyHeaders));
        assertThat(cloudWatchLogsSinkConfig.getHeaderOverrides().isEmpty(), equalTo(true));
    }

    @Test
    void GIVEN_sink_config_with_all_fields_WHEN_accessed_SHOULD_return_all_configured_values() throws NoSuchFieldException, IllegalAccessException {
        Map<String, String> headers = new HashMap<>();
        headers.put("X-Test-Header", "test-value");

        ReflectivelySetField.setField(cloudWatchLogsSinkConfig.getClass(), cloudWatchLogsSinkConfig, "logGroup", LOG_GROUP);
        ReflectivelySetField.setField(cloudWatchLogsSinkConfig.getClass(), cloudWatchLogsSinkConfig, "logStream", LOG_STREAM);
        ReflectivelySetField.setField(cloudWatchLogsSinkConfig.getClass(), cloudWatchLogsSinkConfig, "thresholdConfig", thresholdConfig);
        ReflectivelySetField.setField(cloudWatchLogsSinkConfig.getClass(), cloudWatchLogsSinkConfig, "awsConfig", awsConfig);
        ReflectivelySetField.setField(cloudWatchLogsSinkConfig.getClass(), cloudWatchLogsSinkConfig, "headerOverrides", headers);

        assertThat(cloudWatchLogsSinkConfig.getLogGroup(), equalTo(LOG_GROUP));
        assertThat(cloudWatchLogsSinkConfig.getLogStream(), equalTo(LOG_STREAM));
        assertThat(cloudWatchLogsSinkConfig.getAwsConfig(), equalTo(awsConfig));
        assertThat(cloudWatchLogsSinkConfig.getThresholdConfig(), equalTo(thresholdConfig));
        assertThat(cloudWatchLogsSinkConfig.getHeaderOverrides(), equalTo(headers));
    }

    @Test
    void GIVEN_sink_config_without_header_overrides_WHEN_accessed_SHOULD_maintain_backward_compatibility() throws NoSuchFieldException, IllegalAccessException {
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
        assertThat(cloudWatchLogsSinkConfig.getWorkers(), equalTo(CloudWatchLogsSinkConfig.DEFAULT_NUM_WORKERS));
        assertThat(cloudWatchLogsSinkConfig.getMaxRetries(), equalTo(CloudWatchLogsSinkConfig.DEFAULT_RETRY_COUNT));
    }

    // Custom Headers Validation Tests
    @Test
    void GIVEN_valid_custom_headers_WHEN_validated_THEN_returns_true() {
        Map<String, String> validHeaders = new HashMap<>();
        validHeaders.put("X-Custom-Header", "custom-value");
        validHeaders.put("Content-Type", "application/json");
        
        boolean result = validator.isValid(validHeaders, context);
        
        assertThat(result, equalTo(true));
        verify(context, never()).disableDefaultConstraintViolation();
    }

    @Test
    void GIVEN_empty_custom_headers_WHEN_validated_THEN_returns_true() {
        Map<String, String> emptyHeaders = new HashMap<>();
        
        boolean result = validator.isValid(emptyHeaders, context);
        
        assertThat(result, equalTo(true));
        verify(context, never()).disableDefaultConstraintViolation();
    }

    @Test
    void GIVEN_null_custom_headers_WHEN_validated_THEN_returns_true() {
        boolean result = validator.isValid(null, context);
        
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

    @Test
    void GIVEN_invalid_header_name_with_spaces_WHEN_validated_THEN_returns_false() {
        Map<String, String> headersWithInvalidName = new HashMap<>();
        headersWithInvalidName.put("Invalid Header Name", "valid-value");
        
        boolean result = validator.isValid(headersWithInvalidName, context);
        
        assertThat(result, equalTo(false));
        verify(context).disableDefaultConstraintViolation();
        verify(context).buildConstraintViolationWithTemplate("Invalid header name 'Invalid Header Name'");
    }

    @Test
    void GIVEN_invalid_header_name_with_special_chars_WHEN_validated_THEN_returns_false() {
        Map<String, String> headersWithInvalidName = new HashMap<>();
        headersWithInvalidName.put("Header@Name", "valid-value");
        
        boolean result = validator.isValid(headersWithInvalidName, context);
        
        assertThat(result, equalTo(false));
        verify(context).disableDefaultConstraintViolation();
        verify(context).buildConstraintViolationWithTemplate("Invalid header name 'Header@Name'");
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

    // Size Validation Tests
    @Test
    void GIVEN_exactly_10_header_overrides_WHEN_accessed_THEN_should_be_valid() throws Exception {
        CloudWatchLogsSinkConfig config = new CloudWatchLogsSinkConfig();
        Map<String, String> exactlyTenHeaders = new HashMap<>();
        
        for (int i = 1; i <= 10; i++) {
            exactlyTenHeaders.put("Header-" + i, "value-" + i);
        }
        
        ReflectivelySetField.setField(config.getClass(), config, "headerOverrides", exactlyTenHeaders);
        
        assertThat(config.getHeaderOverrides(), aMapWithSize(10));
        assertThat(config.getHeaderOverrides().size(), lessThanOrEqualTo(10));
    }

    @Test
    void GIVEN_less_than_10_header_overrides_WHEN_accessed_THEN_should_be_valid() throws Exception {
        CloudWatchLogsSinkConfig config = new CloudWatchLogsSinkConfig();
        Map<String, String> fiveHeaders = new HashMap<>();
        
        for (int i = 1; i <= 5; i++) {
            fiveHeaders.put("Header-" + i, "value-" + i);
        }
        
        ReflectivelySetField.setField(config.getClass(), config, "headerOverrides", fiveHeaders);
        
        assertThat(config.getHeaderOverrides(), aMapWithSize(5));
        assertThat(config.getHeaderOverrides().size(), lessThanOrEqualTo(10));
    }

    @Test
    void GIVEN_default_config_WHEN_accessed_THEN_header_overrides_should_be_empty() {
        CloudWatchLogsSinkConfig config = new CloudWatchLogsSinkConfig();
        
        assertThat(config.getHeaderOverrides(), aMapWithSize(0));
        assertThat(config.getHeaderOverrides().size(), lessThanOrEqualTo(10));
    }

    @Test
    void GIVEN_new_sink_config_WHEN_get_endpoint_called_SHOULD_return_null() {
        assertThat(new CloudWatchLogsSinkConfig().getEndpoint(), equalTo(null));
    }

    @Test
    void GIVEN_endpoint_configured_SHOULD_return_the_configured_value() throws NoSuchFieldException, IllegalAccessException {
        String testEndpoint = "https://logs.us-west-2.amazonaws.com";
        
        ReflectivelySetField.setField(cloudWatchLogsSinkConfig.getClass(), cloudWatchLogsSinkConfig, "endpoint", testEndpoint);
        assertThat(cloudWatchLogsSinkConfig.getEndpoint(), equalTo(testEndpoint));
    }

    @Test
    void GIVEN_new_sink_config_WHEN_get_entity_called_SHOULD_return_null() {
        assertThat(new CloudWatchLogsSinkConfig().getEntity(), equalTo(null));
    }

    @Test
    void GIVEN_entity_configured_SHOULD_return_the_configured_value() throws NoSuchFieldException, IllegalAccessException {
        EntityConfig testEntity = new EntityConfig();
        
        ReflectivelySetField.setField(cloudWatchLogsSinkConfig.getClass(), cloudWatchLogsSinkConfig, "entity", testEntity);
        assertThat(cloudWatchLogsSinkConfig.getEntity(), equalTo(testEntity));
    }
}
