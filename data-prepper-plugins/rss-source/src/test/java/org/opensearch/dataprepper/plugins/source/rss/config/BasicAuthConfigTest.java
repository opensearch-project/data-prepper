/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.rss.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.MessageInterpolator;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

class BasicAuthConfigTest {

    private ObjectMapper objectMapper;
    private Validator validator;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        // Use a pass-through interpolator so the provider does not require a
        // jakarta EL implementation (which is not on the test classpath).
        final MessageInterpolator interpolator = new MessageInterpolator() {
            @Override
            public String interpolate(final String messageTemplate, final Context context) {
                return messageTemplate;
            }

            @Override
            public String interpolate(final String messageTemplate, final Context context, final Locale locale) {
                return messageTemplate;
            }
        };
        validator = Validation.byDefaultProvider()
                .configure()
                .messageInterpolator(interpolator)
                .buildValidatorFactory()
                .getValidator();
    }

    @Test
    void deserializes_username_and_password() {
        final BasicAuthConfig config = objectMapper.convertValue(
                Map.of("username", "user", "password", "pass"), BasicAuthConfig.class);
        assertThat(config.getUsername(), equalTo("user"));
        assertThat(config.getPassword(), equalTo("pass"));
    }

    @Test
    void username_and_password_present_produces_no_violations() {
        final BasicAuthConfig config = objectMapper.convertValue(
                Map.of("username", "user", "password", "pass"), BasicAuthConfig.class);
        assertThat(validator.validate(config), hasSize(0));
    }

    @Test
    void blank_username_fails_not_blank_constraint() {
        final BasicAuthConfig config = objectMapper.convertValue(
                Map.of("username", "", "password", "pass"), BasicAuthConfig.class);
        final Set<ConstraintViolation<BasicAuthConfig>> violations = validator.validate(config);
        assertThat(violations, hasSize(1));
        assertThat(violations.iterator().next().getPropertyPath().toString(), equalTo("username"));
    }

    @Test
    void missing_password_fails_not_blank_constraint() {
        final BasicAuthConfig config = objectMapper.convertValue(
                Map.of("username", "user"), BasicAuthConfig.class);
        final Set<ConstraintViolation<BasicAuthConfig>> violations = validator.validate(config);
        assertThat(violations, hasSize(1));
        assertThat(violations.iterator().next().getPropertyPath().toString(), equalTo("password"));
    }
}
