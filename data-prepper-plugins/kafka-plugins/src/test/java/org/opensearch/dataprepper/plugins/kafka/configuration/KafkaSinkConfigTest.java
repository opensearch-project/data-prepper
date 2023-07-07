/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.configuration.PluginSetting;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;


class KafkaSinkConfigTest {


    KafkaSinkConfig kafkaSinkConfig;

    List<String> bootstrapServers;

    @BeforeEach
    void setUp() {
        kafkaSinkConfig = new KafkaSinkConfig();
        kafkaSinkConfig.setBootStrapServers(Arrays.asList("127.0.0.1:9093"));
        kafkaSinkConfig.setAuthConfig(mock(AuthConfig.class));
        kafkaSinkConfig.setTopics(Arrays.asList(mock(TopicConfig.class)));
        kafkaSinkConfig.setSchemaConfig((mock(SchemaConfig.class)));
        kafkaSinkConfig.setThreadWaitTime(10L);
        kafkaSinkConfig.setSerdeFormat("JSON");

    }

    @Test
    void test_kafka_config_not_null() {
        assertThat(kafkaSinkConfig, notNullValue());
    }

    @Test
    void test_bootStrapServers_not_null() {
        assertThat(kafkaSinkConfig.getBootStrapServers(), notNullValue());
        List<String> servers = kafkaSinkConfig.getBootStrapServers();
        bootstrapServers = servers.stream().
                flatMap(str -> Arrays.stream(str.split(","))).
                map(String::trim).
                collect(Collectors.toList());
        assertThat(bootstrapServers.size(), equalTo(1));
        assertThat(bootstrapServers, hasItem("127.0.0.1:9093"));
    }

    @Test
    void test_topics_not_null() {
        assertThat(kafkaSinkConfig.getTopics(), notNullValue());
    }

    @Test
    void test_schema_not_null() {
        assertThat(kafkaSinkConfig.getSchemaConfig(), notNullValue());
    }

    @Test
    void test_authentication_not_null() {
        assertThat(kafkaSinkConfig.getAuthConfig(), notNullValue());
    }

    @Test
    void test_serde_format_not_null() {
        assertThat(kafkaSinkConfig.getSerdeFormat(), notNullValue());
    }

    @Test
    void test_thread_wait_time_null() {
        assertThat(kafkaSinkConfig.getThreadWaitTime(), notNullValue());
    }

    @Test
    public void testDLQConfiguration() {
        final Map<String, Object> fakePlugin = new LinkedHashMap<>();
        final Map<String, Object> lowLevelPluginSettings = new HashMap<>();
        lowLevelPluginSettings.put("field1", "value1");
        lowLevelPluginSettings.put("field2", "value2");
        fakePlugin.put("another_dlq", lowLevelPluginSettings);
        kafkaSinkConfig.setDlqConfig(generatePluginSetting(fakePlugin));
        assertEquals("another_dlq", kafkaSinkConfig.getDlq().get().getPluginName());
    }

    private PluginSetting generatePluginSetting(final Map<String, Object> pluginSettings) {
        final Map<String, Object> metadata = new HashMap<>();
        if (pluginSettings != null) {
            metadata.put(KafkaSinkConfig.DLQ, pluginSettings);
        }

        return new PluginSetting("S3", metadata);
    }
}
