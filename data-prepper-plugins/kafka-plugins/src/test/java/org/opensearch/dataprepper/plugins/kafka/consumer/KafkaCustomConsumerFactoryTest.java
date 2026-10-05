/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.kafka.consumer;

import org.junit.jupiter.api.Test;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConsumerConfig;

import java.lang.reflect.Constructor;
import java.time.Duration;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.test.helper.ReflectivelySetField.setField;

class KafkaCustomConsumerFactoryTest {

    // SourceTopicConfig is package-private to the source package, so it is referenced reflectively here.
    private static final String SOURCE_TOPIC_CONFIG_CLASS_NAME =
            "org.opensearch.dataprepper.plugins.kafka.source.SourceTopicConfig";

    private TopicConsumerConfig createSourceTopicConfig() throws Exception {
        final Class<?> sourceTopicConfigClass = Class.forName(SOURCE_TOPIC_CONFIG_CLASS_NAME);
        final Constructor<?> constructor = sourceTopicConfigClass.getDeclaredConstructor();
        constructor.setAccessible(true);
        final TopicConsumerConfig topicConfig = (TopicConsumerConfig) constructor.newInstance();
        // group_id is required by the method; other getters return their defaults.
        setField(sourceTopicConfigClass, topicConfig, "groupId", "test-group");
        return topicConfig;
    }

    @Test
    void setConsumerTopicProperties_sets_connections_max_idle_from_source_config() throws Exception {
        final TopicConsumerConfig topicConfig = createSourceTopicConfig();
        setField(topicConfig.getClass(), topicConfig, "connectionsMaxIdle", Duration.ofSeconds(180));

        final Properties properties = new Properties();
        KafkaCustomConsumerFactory.setConsumerTopicProperties(properties, topicConfig, topicConfig.getGroupId());

        final Object value = properties.get(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG);
        assertThat(value, instanceOf(Long.class));
        assertThat(value, equalTo(180_000L));
    }

    @Test
    void setConsumerTopicProperties_defaults_connections_max_idle_to_540000ms() throws Exception {
        final TopicConsumerConfig topicConfig = createSourceTopicConfig();

        final Properties properties = new Properties();
        KafkaCustomConsumerFactory.setConsumerTopicProperties(properties, topicConfig, topicConfig.getGroupId());

        assertThat(properties.get(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG), equalTo(540_000L));
    }

    @Test
    void setConsumerTopicProperties_omits_connections_max_idle_when_null() {
        final TopicConsumerConfig topicConfig = mock(TopicConsumerConfig.class);
        when(topicConfig.getConnectionsMaxIdle()).thenReturn(null);
        when(topicConfig.getMaxPartitionFetchBytes()).thenReturn(1024L);
        when(topicConfig.getRetryBackoff()).thenReturn(Duration.ofSeconds(10));
        when(topicConfig.getReconnectBackoff()).thenReturn(Duration.ofSeconds(10));
        when(topicConfig.getAutoCommit()).thenReturn(false);
        when(topicConfig.getCommitInterval()).thenReturn(Duration.ofSeconds(5));
        when(topicConfig.getAutoOffsetReset()).thenReturn("earliest");
        when(topicConfig.getConsumerMaxPollRecords()).thenReturn(500);
        when(topicConfig.getMaxPollInterval()).thenReturn(Duration.ofSeconds(300));
        when(topicConfig.getSessionTimeOut()).thenReturn(Duration.ofSeconds(45));
        when(topicConfig.getHeartBeatInterval()).thenReturn(Duration.ofSeconds(5));
        when(topicConfig.getFetchMaxBytes()).thenReturn(52428800L);
        when(topicConfig.getFetchMaxWait()).thenReturn(500);
        when(topicConfig.getFetchMinBytes()).thenReturn(1L);

        final Properties properties = new Properties();
        KafkaCustomConsumerFactory.setConsumerTopicProperties(properties, topicConfig, "buffer-group");

        assertThat(properties.get(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG), nullValue());
    }
}
