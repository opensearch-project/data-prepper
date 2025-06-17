/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.sink.otlp.configuration;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.types.ByteCount;

import java.io.IOException;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ThresholdConfigTest {

    private static ObjectMapper mapper;

    private static final int CUSTOM_MAX_EVENTS = 100;
    private static final String CUSTOM_MAX_BATCH_SIZE = "2mb";
    private static final Duration CUSTOM_FLUSH_TIMEOUT = Duration.ofMillis(500);

    private static final int DEFAULT_MAX_EVENTS = 512;
    private static final String DEFAULT_MAX_BATCH_SIZE = "1mb";
    private static final Duration DEFAULT_FLUSH_TIMEOUT = Duration.ofMillis(200);

    @BeforeAll
    static void setupMapper() {
        mapper = new ObjectMapper(new YAMLFactory())
                .findAndRegisterModules();  // for Duration

        // Register a simple deserializer for ByteCount.from “2mb”-style strings
        final SimpleModule byteCountModule = new SimpleModule();
        byteCountModule.addDeserializer(ByteCount.class, new JsonDeserializer<>() {
            @Override
            public ByteCount deserialize(final JsonParser p, final DeserializationContext ctxt) throws IOException {
                return ByteCount.parse(p.getValueAsString());
            }
        });
        mapper.registerModule(byteCountModule);
    }

    @Test
    void testDeserializationFromYaml() throws Exception {
        final String yaml = String.join("\n",
                "max_events: " + CUSTOM_MAX_EVENTS,
                "max_batch_size: \"" + CUSTOM_MAX_BATCH_SIZE + "\"",
                "flush_timeout: \"" + CUSTOM_FLUSH_TIMEOUT.toString() + "\""
        );

        final ThresholdConfig config = mapper.readValue(yaml, ThresholdConfig.class);

        assertEquals(CUSTOM_MAX_EVENTS, config.getMaxEvents());
        assertEquals(ByteCount.parse(CUSTOM_MAX_BATCH_SIZE), config.getMaxBatchSize());
        assertEquals(CUSTOM_FLUSH_TIMEOUT, config.getFlushTimeout());
    }

    @Test
    void testDefaultsWhenYamlEmpty() throws Exception {
        final ThresholdConfig config = mapper.readValue("{}", ThresholdConfig.class);

        assertEquals(DEFAULT_MAX_EVENTS, config.getMaxEvents());
        assertEquals(ByteCount.parse(DEFAULT_MAX_BATCH_SIZE), config.getMaxBatchSize());
        assertEquals(DEFAULT_FLUSH_TIMEOUT, config.getFlushTimeout());
    }
}
