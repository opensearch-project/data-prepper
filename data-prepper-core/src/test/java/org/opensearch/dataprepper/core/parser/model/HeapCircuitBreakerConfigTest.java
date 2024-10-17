/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.parser.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.core.parser.model.HeapCircuitBreakerConfig;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.pipeline.parser.ByteCountDeserializer;
import org.opensearch.dataprepper.pipeline.parser.DataPrepperDurationDeserializer;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class HeapCircuitBreakerConfigTest {
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper(new YAMLFactory());

        final SimpleModule simpleModule = new SimpleModule()
                .addDeserializer(ByteCount.class, new ByteCountDeserializer())
                .addDeserializer(Duration.class, new DataPrepperDurationDeserializer());
        objectMapper.registerModule(simpleModule);
    }

    @Test
    void deserialize_heap_without_reset() throws IOException {
        final InputStream resourceStream = this.getClass().getResourceAsStream("heap_with_reset.yaml");

        final HeapCircuitBreakerConfig config = objectMapper.readValue(resourceStream, HeapCircuitBreakerConfig.class);

        assertThat(config, notNullValue());
        assertThat(config.getUsage(), notNullValue());
        assertThat(config.getUsage().getBytes(), equalTo(24L));
        assertThat(config.getReset(), notNullValue());
        assertThat(config.getReset(), equalTo(Duration.ofSeconds(3)));
    }

    @Test
    void deserialize_heap_without_reset_configured() throws IOException {
        final InputStream resourceStream = this.getClass().getResourceAsStream("heap_without_reset.yaml");

        final HeapCircuitBreakerConfig config = objectMapper.readValue(resourceStream, HeapCircuitBreakerConfig.class);

        assertThat(config, notNullValue());
        assertThat(config.getUsage(), notNullValue());
        assertThat(config.getUsage().getBytes(), equalTo(24L));
        assertThat(config.getReset(), notNullValue());
        assertThat(config.getReset(), equalTo(HeapCircuitBreakerConfig.DEFAULT_RESET));
    }
}