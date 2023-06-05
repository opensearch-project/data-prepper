/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.consumer;

import org.apache.avro.generic.GenericRecord;
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
 * * A helper class which helps to process the avro records and write them into the buffer.
 * Offset handling and consumer re-balance are also being handled
 */

@SuppressWarnings("deprecation")
public class AvroConsumer implements KafkaSourceConsumer<String, GenericRecord>, ConsumerRebalanceListener {
    private static final Logger LOG = LoggerFactory.getLogger(AvroConsumer.class);
    private Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
    private long lastReadOffset = 0L;
    private KafkaConsumer<String, GenericRecord> kafkaAvroConsumer;
    private volatile long lastCommitTime = System.currentTimeMillis();
    final KafkaConsumer<String, GenericRecord> consumer;
    final AtomicBoolean status;
    final Buffer<Record<Object>> buffer;
    final TopicConfig topicConfig;
    final KafkaSourceConfig kafkaSourceConfig;
    PluginMetrics pluginMetrics;
    final String schemaType;

    public AvroConsumer(KafkaConsumer<String, GenericRecord> consumer,
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

    @Override
    public void consumeRecords() {
        KafkaSourceBufferAccumulator kafkaSourceBufferAccumulator = new KafkaSourceBufferAccumulator(topicConfig,kafkaSourceConfig,
                schemaType,pluginMetrics);
        kafkaAvroConsumer = consumer;
        try {
            consumer.subscribe(Arrays.asList(topicConfig.getName()));
            while (!status.get()) {
                offsetsToCommit.clear();
                ConsumerRecords<String, GenericRecord> records = poll(consumer);
                if (!records.isEmpty() && records.count() > 0) {
                    iterateRecordPartitions(kafkaSourceBufferAccumulator, records);
                    if (!offsetsToCommit.isEmpty() && topicConfig.getAutoCommit().equalsIgnoreCase("false")) {
                        lastCommitTime = kafkaSourceBufferAccumulator.commitOffsets(consumer, lastCommitTime, offsetsToCommit);
                    }
                }
            }
        } catch (Exception exp) {
            LOG.error("Error while reading avro records from the topic...", exp);
        }
    }

    private void iterateRecordPartitions(KafkaSourceBufferAccumulator kafkaSourceBufferAccumulator,
                                         ConsumerRecords<String, GenericRecord> records) throws Exception {
        for (TopicPartition partition : records.partitions()) {
            List<Record<Object>> kafkaRecords = new ArrayList<>();
            List<ConsumerRecord<String, GenericRecord>> partitionRecords = records.records(partition);
            iterateConsumerRecords(kafkaSourceBufferAccumulator, kafkaRecords, partitionRecords);
            if (!kafkaRecords.isEmpty()) {
                kafkaSourceBufferAccumulator.writeAllRecordToBuffer(kafkaRecords, buffer, topicConfig);
            }
        }
    }

    private void iterateConsumerRecords(KafkaSourceBufferAccumulator kafkaSourceBufferAccumulator,
                                        List<Record<Object>> kafkaRecords,
                                        List<ConsumerRecord<String, GenericRecord>> partitionRecords) {
        for (ConsumerRecord<String, GenericRecord> consumerRecord : partitionRecords) {
            lastReadOffset = kafkaSourceBufferAccumulator.processConsumerRecords(offsetsToCommit, kafkaRecords, lastReadOffset, consumerRecord, partitionRecords);
        }
    }

    private ConsumerRecords<String, GenericRecord> poll(final KafkaConsumer<String, GenericRecord> consumer){
        return consumer.poll(Duration.ofMillis(100));
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        LOG.trace("onPartitionsAssigned() callback triggered and Closing the consumer...");
        for (TopicPartition partition : partitions) {
            kafkaAvroConsumer.seek(partition, lastReadOffset);
        }
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        LOG.trace("onPartitionsRevoked() callback triggered and Committing the offsets: {} ", offsetsToCommit);
        try {
            kafkaAvroConsumer.commitSync(offsetsToCommit);
        } catch (CommitFailedException e) {
            LOG.error("Failed to commit the record...", e);
        }
    }
}
