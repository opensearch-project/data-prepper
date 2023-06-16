/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
//import org.junit.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Properties;


public class PlainTextConsumerIT {

    private PluginMetrics pluginMetrics;

    private Buffer<Record<Object>> buffer;

    @ClassRule
    public static final EmbeddedKafkaClusterSingleNode CLUSTER = new EmbeddedKafkaClusterSingleNode();

    @BeforeClass
    public static void createTopics() {
        CLUSTER.createTopic("test-IT-topic");
    }

    @Before
    public void configure() {
/*
        List<TopicsConfig> list = new ArrayList<TopicsConfig>();
        TopicConfig topicConfig = new TopicConfig();
        buffer = mock(Buffer.class);
        kafkaSourceConfig = new KafkaSourceConfig();
        topicsConfig = new TopicsConfig();
        consumerConfigs = new ConsumerGroupConfig();
        schemaConfig = new SchemaConfig();
        schemaConfig.setKeyDeserializer("org.apache.kafka.common.serialization.StringDeserializer");
        schemaConfig.setValueDeserializer("org.apache.kafka.common.serialization.StringDeserializer");
        schemaConfig.setSchemaType("plaintext");
        schemaConfig.setRegistryURL(CLUSTER.schemaRegistryUrl());
        consumerConfigs.setAutoCommitInterval(Duration.ofMillis(1000));
        consumerConfigs.setAutoOffsetReset("earliest");
        consumerConfigs.setAutoCommit("false");
        consumerConfigs.setGroupId("DPKafkaProj");
        consumerConfigs.setGroupName("DPKafkaProj-1");
        consumerConfigs.setWorkers(3);

        topicConfig.setConsumerGroupConfig(consumerConfigs);
        topicConfig.setSchemaConfig(schemaConfig);
        topicConfig.setTopic("test-IT-topic");

        topicsConfig.setTopics(topicConfig);
        list.add(topicsConfig);
        kafkaSourceConfig.setTopics(list);
        kafkaSourceConfig.setBootStrapServers(Arrays.asList(CLUSTER.bootstrapServers(), "127.0.0.1:9092"));
        pluginMetrics = mock(PluginMetrics.class);
        kafkaSource = new KafkaSource(kafkaSourceConfig, pluginMetrics);*/
    }

    @Test
    public void consumeKafkaMessages_should_return_at_least_one_message() {
        produceTestMessages();
//        kafkaSource.start(buffer);
    }

    private void produceTestMessages() {

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, CLUSTER.schemaRegistryUrl());
        try (KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props)) {
            for (long i = 0; i < 10; i++) {
                producer.send(new ProducerRecord<>("test-IT-topic",
                        "hello" + i));
                Thread.sleep(1000L);
            }
            producer.flush();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}