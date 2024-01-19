/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Properties;
import java.util.UUID;

/**
 * Utility class to produce data to Kafka to help with testing.
 */
public class TestProducer {
    private final Producer<byte[], byte[]> kafkaProducer;
    private final String topicName;

    public TestProducer(final String bootstrapServersCommaDelimited, final String topicName) {
        this.topicName = topicName;
        final String testGroupId = UUID.randomUUID().toString();
        final Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersCommaDelimited);
        kafkaProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, testGroupId);
        kafkaProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaProducer = new KafkaProducer<>(kafkaProperties, new ByteArraySerializer(), new ByteArraySerializer());
        kafkaProducer.flush();
    }

    /**
     * Publishes a single record to Kafka.
     *
     * @param key The key as a byte[]
     * @param value The value as a byte[]
     */
    public void publishRecord(final byte[] key, final byte[] value) {
        final ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(topicName, key, value);
        kafkaProducer.send(producerRecord);
        kafkaProducer.flush();
    }
}
