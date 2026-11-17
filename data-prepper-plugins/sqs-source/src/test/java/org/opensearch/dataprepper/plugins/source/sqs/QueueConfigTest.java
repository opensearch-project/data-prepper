/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.sqs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.hasToString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

public class QueueConfigTest {

    private static final ObjectMapper YAML_MAPPER =
            new ObjectMapper(new YAMLFactory()).registerModule(new JavaTimeModule());

    @Test
    void testDefaultValues() {
        final QueueConfig queueConfig = new QueueConfig();

        assertNull(queueConfig.getUrl(), "URL should be null by default");
        assertEquals(1, queueConfig.getNumWorkers(), "Number of workers should default to 1");
        assertNull(queueConfig.getMaximumMessages(), "Maximum messages should be null by default");
        assertEquals(Duration.ofSeconds(0), queueConfig.getPollDelay(), "Poll delay should default to 0 seconds");
        assertNull(queueConfig.getCodec(), "Codec should be null by default");
        assertNull(queueConfig.getVisibilityTimeout(), "Visibility timeout should be null by default");
        assertFalse(queueConfig.getVisibilityDuplicateProtection(), "Visibility duplicate protection should default to false");
        assertEquals(Duration.ofHours(2), queueConfig.getVisibilityDuplicateProtectionTimeout(),
                "Visibility duplicate protection timeout should default to 2 hours");
        assertNull(queueConfig.getWaitTime(), "Wait time should default to null");
        assertThat(queueConfig.getMaximumBackoff(), equalTo(Duration.ofMinutes(5)));
    }

    @Test
    void testMaximumBackoffReadsConfiguredValue() throws Exception {
        final String yaml = "url: \"https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue\"\n"
                + "maximum_backoff: \"PT10M\"\n";

        final QueueConfig queueConfig = YAML_MAPPER.readValue(yaml, QueueConfig.class);

        assertThat(queueConfig.getMaximumBackoff(), equalTo(Duration.ofMinutes(10)));
    }

    @Test
    void testMaximumBackoffBelowMinimumFailsValidation() throws Exception {
        final String yaml = "url: \"https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue\"\n"
                + "maximum_backoff: \"PT10S\"\n";

        final QueueConfig queueConfig = YAML_MAPPER.readValue(yaml, QueueConfig.class);

        // ParameterMessageInterpolator avoids requiring a Jakarta EL provider on the test classpath.
        try (final ValidatorFactory factory = Validation.byDefaultProvider().configure()
                .messageInterpolator(new ParameterMessageInterpolator())
                .buildValidatorFactory()) {
            final Validator validator = factory.getValidator();
            final Set<ConstraintViolation<QueueConfig>> violations = validator.validate(queueConfig);
            assertThat(violations, hasItem(hasProperty("propertyPath", hasToString("maximumBackoff"))));
        }
    }
}
