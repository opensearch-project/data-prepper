/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Max;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import jakarta.validation.Constraint;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import jakarta.validation.Payload;

import org.opensearch.dataprepper.model.configuration.PluginModel;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class CloudWatchLogsSinkConfig {
    public static final int DEFAULT_RETRY_COUNT = 5;
    public static final int DEFAULT_NUM_WORKERS = 10;

    @JsonProperty("aws")
    @Valid
    private AwsConfig awsConfig;

    @JsonProperty("dlq")
    private PluginModel dlq;

    @JsonProperty("threshold")
    private ThresholdConfig thresholdConfig = new ThresholdConfig();

    @JsonProperty("log_group")
    @NotEmpty
    @NotNull
    private String logGroup;

    @JsonProperty("log_stream")
    @NotEmpty
    @NotNull
    private String logStream;

    @JsonProperty(value = "max_retries", defaultValue = "5")
    @Min(1)
    @Max(15)
    private int maxRetries = DEFAULT_RETRY_COUNT;

    @JsonProperty(value = "workers", defaultValue = "10")
    @Min(1)
    @Max(50)
    private int workers = DEFAULT_NUM_WORKERS;

    @JsonProperty("custom_headers")
    @Size(max = 10, message = "Maximum 10 custom headers allowed")
    @ValidCustomHeaders
    private Map<String, String> customHeaders = new HashMap<>();

    public AwsConfig getAwsConfig() {
        return awsConfig;
    }

    public ThresholdConfig getThresholdConfig() {
        return thresholdConfig;
    }

    public PluginModel getDlq() {
        return dlq;
    }

    public String getLogGroup() {
        return logGroup;
    }

    public String getLogStream() {
        return logStream;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public int getWorkers() {
        return workers;
    }

    public Map<String, String> getCustomHeaders() {
        return customHeaders;
    }

}

/**
 * Custom validation annotation for HTTP headers.
 */
@Documented
@Constraint(validatedBy = ValidCustomHeaders.CustomHeadersValidator.class)
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@interface ValidCustomHeaders {
    String message() default "Invalid custom headers configuration";
    Class<?>[] groups() default {};
    Class<? extends Payload>[] payload() default {};
    
    /**
     * Custom validator for HTTP headers.
     */
    class CustomHeadersValidator implements ConstraintValidator<ValidCustomHeaders, Map<String, String>> {
        
        private static final Pattern VALID_HEADER_NAME_PATTERN = 
            Pattern.compile("^[!#$%&'*+\\-.0-9A-Z^_`a-z|~]+$");
        
        @Override
        public void initialize(ValidCustomHeaders constraintAnnotation) {
            // No initialization needed
        }
        
        @Override
        public boolean isValid(Map<String, String> headers, ConstraintValidatorContext context) {
            if (headers == null || headers.isEmpty()) {
                return true; // Empty headers are valid
            }
            
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                String headerName = entry.getKey();
                String headerValue = entry.getValue();
                
                // Check for null header name
                if (headerName == null) {
                    context.disableDefaultConstraintViolation();
                    context.buildConstraintViolationWithTemplate("Header name cannot be null")
                           .addConstraintViolation();
                    return false;
                }
                
                // Check for null header value
                if (headerValue == null) {
                    context.disableDefaultConstraintViolation();
                    context.buildConstraintViolationWithTemplate("Header '" + headerName + "' has an empty value")
                           .addConstraintViolation();
                    return false;
                }
                
                // Validate header name format
                if (!VALID_HEADER_NAME_PATTERN.matcher(headerName).matches()) {
                    context.disableDefaultConstraintViolation();
                    context.buildConstraintViolationWithTemplate("Invalid header name '" + headerName + "'")
                           .addConstraintViolation();
                    return false;
                }
                
                // Validate header value is not empty
                if (headerValue.trim().isEmpty()) {
                    context.disableDefaultConstraintViolation();
                    context.buildConstraintViolationWithTemplate("Header '" + headerName + "' has an empty value")
                           .addConstraintViolation();
                    return false;
                }
            }
            
            return true;
        }
    }
}
