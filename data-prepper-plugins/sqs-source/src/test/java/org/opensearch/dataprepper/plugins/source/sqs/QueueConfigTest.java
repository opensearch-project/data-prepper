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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        assertEquals(Duration.ofMinutes(5), queueConfig.getMaximumBackoff(),
                "Maximum backoff should default to 5 minutes");
    }

    @Test
    void testMaximumBackoffReadsConfiguredValue() throws Exception {
        final String yaml = "url: \"https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue\"\n"
                + "maximum_backoff: \"PT10M\"\n";

        final QueueConfig queueConfig = YAML_MAPPER.readValue(yaml, QueueConfig.class);

        assertEquals(Duration.ofMinutes(10), queueConfig.getMaximumBackoff(),
                "Maximum backoff should reflect the configured value");
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
            assertTrue(
                    violations.stream().anyMatch(v -> v.getPropertyPath().toString().equals("maximumBackoff")),
                    "Expected a validation violation on maximumBackoff for a value below 20 seconds");
        }
    }
}
