/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * * A Multithreaded helper class which helps to process the records from multiple topics in an
 * asynchronous way.
 */
@SuppressWarnings("deprecation")
public class MultithreadedConsumer implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(MultithreadedConsumer.class);
    private final AtomicBoolean status = new AtomicBoolean(false);
    private final KafkaSourceConfig sourceConfig;
    private final TopicConfig topicConfig;
    private final Buffer<Record<Object>> buffer;
    private final KafkaSourceCustomConsumer customConsumer = new KafkaSourceCustomConsumer();
    private String consumerId;
    private String consumerGroupId;
    private String schemaType;
    private Properties consumerProperties;
    private PluginMetrics pluginMetrics;

    public MultithreadedConsumer(String consumerId,
                                 String consumerGroupId,
                                 Properties properties,
                                 TopicConfig topicConfig,
                                 KafkaSourceConfig sourceConfig,
                                 Buffer<Record<Object>> buffer,
                                 PluginMetrics pluginMetric,
                                 String schemaType) {
        this.consumerProperties = Objects.requireNonNull(properties);
        this.consumerId = consumerId;
        this.consumerGroupId = consumerGroupId;
        this.sourceConfig = sourceConfig;
        this.topicConfig = topicConfig;
        this.buffer = buffer;
        this.schemaType = schemaType;
        this.pluginMetrics = pluginMetric;
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public void run() {
        LOG.info("Consumer group : {} and Consumer : {} executed on : {}", consumerGroupId, consumerId, LocalDateTime.now());
        try {
            MessageFormat schema = MessageFormat.getByMessageFormatByName(schemaType);
            switch (schema) {
                case JSON:
                    new KafkaSourceCustomConsumer(new KafkaConsumer<String, JsonNode>(consumerProperties), status, buffer, topicConfig, sourceConfig, schemaType, pluginMetrics).consumeRecords();
                    break;
                case AVRO:
                    new KafkaSourceCustomConsumer(new KafkaConsumer<String, GenericRecord>(consumerProperties), status, buffer, topicConfig, sourceConfig, schemaType, pluginMetrics).consumeRecords();
                    break;
                case PLAINTEXT:
                default:
                    new KafkaSourceCustomConsumer(new KafkaConsumer<String, String>(consumerProperties), status, buffer, topicConfig, sourceConfig, schemaType, pluginMetrics).consumeRecords();
                    break;
            }

        } catch (Exception exp) {
            if (exp.getCause() instanceof WakeupException && !status.get()) {
                LOG.error("Error reading records from the topic...{}", exp.getMessage());
            }
        } finally {
            LOG.info("Closing the consumer... {}", consumerId);
            closeConsumers();
        }
    }

    private void closeConsumers() {
        customConsumer.closeConsumer();
    }

    public void shutdownConsumer() {
        status.set(false);
        customConsumer.shutdownConsumer();
    }
}
