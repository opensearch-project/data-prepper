/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.consumer;


import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.source.KafkaSourceBufferAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * * A helper class which helps to process the JSON records and write them into
 * the buffer. Offset handling and consumer re-balance are also being handled
 */

@SuppressWarnings("deprecation")
public class JsonConsumer implements KafkaSourceConsumer<String, JsonNode>, ConsumerRebalanceListener {
    private static final Logger LOG = LoggerFactory.getLogger(JsonConsumer.class);
    private Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
    private KafkaConsumer<String, JsonNode> kafkaJsonConsumer;
    private long lastReadOffset = 0L;
    private volatile long lastCommitTime = System.currentTimeMillis();
    final KafkaConsumer<String, JsonNode> consumer;
    final AtomicBoolean status;
    final Buffer<Record<Object>> buffer;
    final TopicConfig topicConfig;
    final KafkaSourceConfig kafkaSourceConfig;
    PluginMetrics pluginMetrics;
    final String schemaType;

    public JsonConsumer(KafkaConsumer<String,JsonNode> consumer,
                        AtomicBoolean status,
                        Buffer<Record<Object>> buffer,
                        TopicConfig topicConfig,
                        KafkaSourceConfig kafkaSourceConfig,
                        String schemaType,
                        PluginMetrics pluginMetrics) {
        this.consumer = consumer;
        this.status = status;
        this.buffer = buffer;
        this.topicConfig = topicConfig;
        this.kafkaSourceConfig = kafkaSourceConfig;
        this.schemaType = schemaType;
        this.pluginMetrics = pluginMetrics;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public void consumeRecords() {
        KafkaSourceBufferAccumulator kafkaSourceBufferAccumulator = new KafkaSourceBufferAccumulator(topicConfig,kafkaSourceConfig,
                schemaType,pluginMetrics);
        kafkaJsonConsumer = consumer;
        try {
            consumer.subscribe(Arrays.asList(topicConfig.getName()));
            while (!status.get()) {
                offsetsToCommit.clear();
                ConsumerRecords<String, JsonNode> records = poll(consumer);
                if (!records.isEmpty() && records.count() > 0) {
                    iterateRecordPartitions(kafkaSourceBufferAccumulator, records);
                }
            }
        } catch (Exception exp) {
            LOG.error("Error while reading the json records from the topic...", exp);
        }
    }

    private void iterateRecordPartitions(KafkaSourceBufferAccumulator kafkaSourceBufferAccumulator, ConsumerRecords<String, JsonNode> records) throws Exception {
        for (TopicPartition partition : records.partitions()) {
            List<Record<Object>> kafkaRecords = new ArrayList<>();
            List<ConsumerRecord<String, JsonNode>> partitionRecords = records.records(partition);
            iterateConsumerRecords(kafkaSourceBufferAccumulator, kafkaRecords, partitionRecords);
            if (!kafkaRecords.isEmpty()) {
                kafkaSourceBufferAccumulator.writeAllRecordToBuffer(kafkaRecords, buffer, topicConfig);
            }
        }
        if (!offsetsToCommit.isEmpty() && topicConfig.getAutoCommit().equalsIgnoreCase("false")) {
            lastCommitTime = kafkaSourceBufferAccumulator.commitOffsets(consumer, lastCommitTime, offsetsToCommit);
        }
    }

    private void iterateConsumerRecords(KafkaSourceBufferAccumulator kafkaSourceBufferAccumulator, List<Record<Object>> kafkaRecords, List<ConsumerRecord<String, JsonNode>> partitionRecords) {
        for (ConsumerRecord<String, JsonNode> consumerRecord : partitionRecords) {
            lastReadOffset = kafkaSourceBufferAccumulator.processConsumerRecords(offsetsToCommit, kafkaRecords, lastReadOffset, consumerRecord, partitionRecords);
        }
    }

    private ConsumerRecords<String, JsonNode> poll(final KafkaConsumer<String, JsonNode> consumer){
        return consumer.poll(Duration.ofMillis(100));
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        LOG.trace("onPartitionsAssigned() callback triggered and Closing the Json consumer...");
        for (TopicPartition partition : partitions) {
            kafkaJsonConsumer.seek(partition, lastReadOffset);
        }
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        LOG.trace("onPartitionsRevoked() callback triggered and Committing the offsets for Json consumer: {} ",
                offsetsToCommit);
        try {
            kafkaJsonConsumer.commitSync(offsetsToCommit);
        } catch (CommitFailedException e) {
            LOG.error("Failed to commit the record for the Json consumer...", e);
        }
    }
}
