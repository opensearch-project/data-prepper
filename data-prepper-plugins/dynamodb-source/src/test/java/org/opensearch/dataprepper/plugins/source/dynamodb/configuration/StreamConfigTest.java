/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.configuration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.hibernate.validator.messageinterpolation.ParameterMessageInterpolator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;
import software.amazon.awssdk.services.dynamodb.model.StreamViewType;

import java.time.Duration;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

class StreamConfigTest {

    private ObjectMapper objectMapper;
    private Validator validator;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper(new YAMLFactory())
                .registerModule(new JavaTimeModule());
        validator = Validation.byDefaultProvider()
                .configure()
                .messageInterpolator(new ParameterMessageInterpolator())
                .buildValidatorFactory()
                .getValidator();
    }

    @Test
    void test_defaults() throws JsonProcessingException {
        final StreamConfig streamConfig = objectMapper.readValue("{}", StreamConfig.class);

        assertThat(streamConfig.getStartPosition(), equalTo(StreamStartPosition.LATEST));
        assertThat(streamConfig.getStreamViewForRemoves(), equalTo(StreamViewType.NEW_IMAGE));
        assertThat(streamConfig.isDisableCheckpointing(), equalTo(false));
        assertThat(streamConfig.getShardDiscoveryInterval(), equalTo(Duration.ofMinutes(1)));
        assertThat(streamConfig.getShardDiscoveryInterval(), equalTo(StreamConfig.DEFAULT_SHARD_DISCOVERY_INTERVAL));
    }

    @Test
    void test_shard_discovery_interval_is_configurable() throws JsonProcessingException {
        final String yaml = "shard_discovery_interval: PT15S";
        final StreamConfig streamConfig = objectMapper.readValue(yaml, StreamConfig.class);

        assertThat(streamConfig.getShardDiscoveryInterval(), equalTo(Duration.ofSeconds(15)));
    }

    @Test
    void test_shard_discovery_interval_accepts_iso8601_minutes() throws JsonProcessingException {
        final String yaml = "shard_discovery_interval: PT5M";
        final StreamConfig streamConfig = objectMapper.readValue(yaml, StreamConfig.class);

        assertThat(streamConfig.getShardDiscoveryInterval(), equalTo(Duration.ofMinutes(5)));
    }

    @Test
    void test_validation_passes_with_default_value() {
        final StreamConfig streamConfig = new StreamConfig();
        final Set<ConstraintViolation<StreamConfig>> violations = validator.validate(streamConfig);

        assertThat(violations, hasSize(0));
    }

    @Test
    void test_validation_passes_at_minimum_boundary() throws NoSuchFieldException, IllegalAccessException {
        final StreamConfig streamConfig = new StreamConfig();
        ReflectivelySetField.setField(StreamConfig.class, streamConfig, "shardDiscoveryInterval", Duration.ofSeconds(1));
        final Set<ConstraintViolation<StreamConfig>> violations = validator.validate(streamConfig);

        assertThat(violations, hasSize(0));
    }

    @Test
    void test_validation_fails_below_minimum() throws NoSuchFieldException, IllegalAccessException {
        final StreamConfig streamConfig = new StreamConfig();
        ReflectivelySetField.setField(StreamConfig.class, streamConfig, "shardDiscoveryInterval", Duration.ofMillis(500));
        final Set<ConstraintViolation<StreamConfig>> violations = validator.validate(streamConfig);

        assertThat(violations, hasSize(1));
    }

    @Test
    void test_validation_fails_when_null() throws NoSuchFieldException, IllegalAccessException {
        final StreamConfig streamConfig = new StreamConfig();
        ReflectivelySetField.setField(StreamConfig.class, streamConfig, "shardDiscoveryInterval", null);
        final Set<ConstraintViolation<StreamConfig>> violations = validator.validate(streamConfig);

        assertThat(violations, hasSize(1));
    }
}
