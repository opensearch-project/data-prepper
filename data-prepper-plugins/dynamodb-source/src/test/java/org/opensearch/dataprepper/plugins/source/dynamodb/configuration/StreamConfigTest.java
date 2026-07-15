/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.dynamodb.configuration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.pipeline.parser.DataPrepperDurationDeserializer;
import org.opensearch.dataprepper.test.helper.ReflectivelySetField;
import software.amazon.awssdk.services.dynamodb.model.StreamViewType;

import java.time.Duration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class StreamConfigTest {

    private final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory())
            .registerModule(new SimpleModule().addDeserializer(Duration.class, new DataPrepperDurationDeserializer()));

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
        final String yaml = "shard_discovery_interval: 15s";
        final StreamConfig streamConfig = objectMapper.readValue(yaml, StreamConfig.class);

        assertThat(streamConfig.getShardDiscoveryInterval(), equalTo(Duration.ofSeconds(15)));
    }

    @Test
    void test_shard_discovery_interval_accepts_iso8601() throws JsonProcessingException {
        final String yaml = "shard_discovery_interval: PT5M";
        final StreamConfig streamConfig = objectMapper.readValue(yaml, StreamConfig.class);

        assertThat(streamConfig.getShardDiscoveryInterval(), equalTo(Duration.ofMinutes(5)));
    }

    @Test
    void test_shard_discovery_interval_validation_accepts_default() {
        final StreamConfig streamConfig = new StreamConfig();
        assertThat(streamConfig.isShardDiscoveryIntervalValid(), equalTo(true));
    }

    @Test
    void test_shard_discovery_interval_validation_accepts_minimum() throws NoSuchFieldException, IllegalAccessException {
        final StreamConfig streamConfig = new StreamConfig();
        ReflectivelySetField.setField(StreamConfig.class, streamConfig, "shardDiscoveryInterval", Duration.ofSeconds(1));
        assertThat(streamConfig.isShardDiscoveryIntervalValid(), equalTo(true));
    }

    @Test
    void test_shard_discovery_interval_validation_rejects_below_minimum() throws NoSuchFieldException, IllegalAccessException {
        final StreamConfig streamConfig = new StreamConfig();
        ReflectivelySetField.setField(StreamConfig.class, streamConfig, "shardDiscoveryInterval", Duration.ofMillis(500));
        assertThat(streamConfig.isShardDiscoveryIntervalValid(), equalTo(false));
    }

    @Test
    void test_shard_discovery_interval_validation_rejects_null() throws NoSuchFieldException, IllegalAccessException {
        final StreamConfig streamConfig = new StreamConfig();
        ReflectivelySetField.setField(StreamConfig.class, streamConfig, "shardDiscoveryInterval", null);
        assertThat(streamConfig.isShardDiscoveryIntervalValid(), equalTo(false));
    }
}
