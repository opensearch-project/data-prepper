/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.cloudwatch_logs.config;

import jakarta.validation.Constraint;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import jakarta.validation.Payload;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Custom validation annotation for HTTP headers.
 */
@Documented
@Constraint(validatedBy = ValidCustomHeaders.CustomHeadersValidator.class)
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface ValidCustomHeaders {
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