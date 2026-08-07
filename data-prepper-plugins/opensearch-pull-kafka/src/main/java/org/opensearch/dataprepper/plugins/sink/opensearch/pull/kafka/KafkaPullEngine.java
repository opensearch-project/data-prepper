/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.pull.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.plugins.sink.opensearch.PullEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

@DataPrepperPlugin(name = "kafka", pluginType = PullEngine.class, pluginConfigurationType = KafkaPullEngineConfig.class,
        packagesToScan = {KafkaPullEngine.class})
public class KafkaPullEngine implements PullEngine {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaPullEngine.class);

    private final String bootstrapServers;
    private final TopicManager topicManager;

    private KafkaProducer<String, byte[]> producer;
    private String topicName;

    @DataPrepperPluginConstructor
    public KafkaPullEngine(final KafkaPullEngineConfig config, final TopicManager topicManager) {
        this.bootstrapServers = String.join(",", config.getBootstrapServers());
        this.topicManager = topicManager;
    }

    @Override
    public void initialize(final String topicName, final int partitionCount) {
        this.topicName = topicName;

        topicManager.createTopicWithPartitions(topicName, partitionCount);

        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");

        this.producer = new KafkaProducer<>(producerProps);
        LOG.info("KafkaPullEngine initialized with topic '{}' and {} partition(s)", topicName, partitionCount);
    }

    @Override
    public void write(final int partition, final String key, final byte[] document) {
        final ProducerRecord<String, byte[]> record = new ProducerRecord<>(topicName, partition, key, document);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                LOG.error("Failed to send record with key '{}' to partition {}", key, partition, exception);
            }
        });
    }

    @Override
    public void flush() {
        if (producer != null) {
            producer.flush();
        }
    }

    @Override
    public void shutdown() {
        if (producer != null) {
            producer.flush();
            producer.close();
        }
    }
}
