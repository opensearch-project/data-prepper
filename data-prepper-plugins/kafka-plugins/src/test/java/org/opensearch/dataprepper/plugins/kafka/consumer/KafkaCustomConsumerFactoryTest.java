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

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.kafka.common.serialization.SerializationFactory;
import org.opensearch.dataprepper.plugins.kafka.configuration.CommonTopicConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaConsumerConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaRegistryType;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConsumerConfig;

import java.lang.reflect.Constructor;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
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

    @ParameterizedTest
    @ValueSource(strings = {"AVRO", "JSON"})
    void createConsumersForTopic_withConfluentSchema_pinsOnlyValueSubjectNameStrategy(final String schemaType) throws Exception {
        final TopicConsumerConfig topicConfig = createSourceTopicConfig();
        setField(CommonTopicConfig.class, topicConfig, "name", "test-topic");
        final KafkaConsumerConfig consumerConfig = mock(KafkaConsumerConfig.class);
        final SchemaConfig schemaConfig = mock(SchemaConfig.class);
        when(consumerConfig.getBootstrapServers()).thenReturn(List.of("localhost:9092"));
        when(consumerConfig.getSchemaConfig()).thenReturn(schemaConfig);
        when(schemaConfig.getType()).thenReturn(SchemaRegistryType.CONFLUENT);
        when(schemaConfig.getRegistryURL()).thenReturn("http://localhost:8081");
        when(schemaConfig.getVersion()).thenReturn(null);
        final SchemaMetadata schemaMetadata = mock(SchemaMetadata.class);
        when(schemaMetadata.getSchemaType()).thenReturn(schemaType);
        final KafkaCustomConsumerFactory factory = new KafkaCustomConsumerFactory(
                mock(SerializationFactory.class), mock(AwsCredentialsSupplier.class));
        final List<Properties> consumerProperties = new ArrayList<>();

        try (final MockedConstruction<CachedSchemaRegistryClient> schemaRegistryClients = mockConstruction(
                CachedSchemaRegistryClient.class, (client, context) ->
                        when(client.getLatestSchemaMetadata("test-topic-value")).thenReturn(schemaMetadata));
             final MockedConstruction<KafkaConsumer> kafkaConsumers = mockConstruction(
                     KafkaConsumer.class, (consumer, context) ->
                             consumerProperties.add((Properties) context.arguments().get(0)))) {
            final List<KafkaCustomConsumer> consumers = factory.createConsumersForTopic(
                    consumerConfig, topicConfig, mock(Buffer.class), mock(PluginMetrics.class),
                    mock(AcknowledgementSetManager.class), null, new AtomicBoolean(), false, null,
                    CompressionOption.NONE, false);

            assertThat(schemaRegistryClients.constructed(), hasSize(1));
            assertThat(kafkaConsumers.constructed(), hasSize(topicConfig.getWorkers()));
            assertThat(consumers, hasSize(topicConfig.getWorkers()));
            assertThat(consumerProperties, hasSize(topicConfig.getWorkers()));
            for (final Properties properties : consumerProperties) {
                assertThat(properties.getProperty(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY),
                        equalTo(TopicNameStrategy.class.getName()));
                assertThat(properties.getProperty(AbstractKafkaSchemaSerDeConfig.KEY_SUBJECT_NAME_STRATEGY), nullValue());
                assertThat(properties.getProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG),
                        equalTo(StringDeserializer.class.getName()));
            }
        }
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
