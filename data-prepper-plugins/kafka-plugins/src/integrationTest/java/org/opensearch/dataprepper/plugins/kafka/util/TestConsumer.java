/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.util;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * Use this class to consume records from the beginning of a Kafka topic for testing purposes.
 */
public class TestConsumer {
    private final Consumer<byte[], byte[]> kafkaConsumer;

    public TestConsumer(final String bootstrapServersCommaDelimited, final String topicName) {
        final String testGroupId = UUID.randomUUID().toString();
        final Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersCommaDelimited);
        kafkaProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, testGroupId);
        kafkaProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaConsumer = new KafkaConsumer<>(kafkaProperties, new ByteArrayDeserializer(), new ByteArrayDeserializer());

        kafkaConsumer.subscribe(Collections.singletonList(topicName));
    }

    public void readConsumerRecords(final List<ConsumerRecord<byte[], byte[]>> consumerRecords) {
        final ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(Duration.ofMillis(200));
        for (final ConsumerRecord<byte[], byte[]> record : records) {
            consumerRecords.add(record);
        }
    }
}
