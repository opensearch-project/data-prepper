/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.CommitFailedException;
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
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * * A utility class which will handle the core Kafka consumer operation.
 */
public class KafkaSourceCustomConsumer implements ConsumerRebalanceListener {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceCustomConsumer.class);
    private Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
    private long lastReadOffset = 0L;
    private volatile long lastCommitTime = System.currentTimeMillis();
    private KafkaConsumer<String, Object> consumer= null;
    private AtomicBoolean status = new AtomicBoolean(false);
    private Buffer<Record<Object>> buffer= null;
    private TopicConfig topicConfig = null;
    private KafkaSourceConfig kafkaSourceConfig= null;
    private PluginMetrics pluginMetrics= null;
    private String schemaType= null;

    public KafkaSourceCustomConsumer() {
    }

    private KafkaSourceBufferAccumulator kafkaSourceBufferAccumulator= null;
    public KafkaSourceCustomConsumer(KafkaConsumer consumer,
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
        kafkaSourceBufferAccumulator=   new KafkaSourceBufferAccumulator(topicConfig, kafkaSourceConfig,
                schemaType, pluginMetrics);
    }


    @SuppressWarnings({"rawtypes", "unchecked"})
    public void consumeRecords() {
        try {
            consumer.subscribe(Arrays.asList(topicConfig.getName()));
           do {
                offsetsToCommit.clear();
                ConsumerRecords<String, Object> records = poll(consumer);
                if (!records.isEmpty() && records.count() > 0) {
                    iterateRecordPartitions(records);
                }
            }while (!status.get());
        } catch (Exception exp) {
            LOG.error("Error while reading the records from the topic...", exp);
        }
    }

      private void iterateRecordPartitions(ConsumerRecords<String, Object> records) throws Exception {
        for (TopicPartition partition : records.partitions()) {
            List<Record<Object>> kafkaRecords = new ArrayList<>();
            List<ConsumerRecord<String, Object>> partitionRecords = records.records(partition);
            iterateConsumerRecords(kafkaRecords, partitionRecords);
            if (!kafkaRecords.isEmpty()) {
                kafkaSourceBufferAccumulator.writeAllRecordToBuffer(kafkaRecords, buffer, topicConfig);
            }
        }
        if (!offsetsToCommit.isEmpty() && topicConfig.getAutoCommit().equalsIgnoreCase("false")) {
            lastCommitTime = kafkaSourceBufferAccumulator.commitOffsets(consumer, lastCommitTime, offsetsToCommit);
        }
    }

    private void iterateConsumerRecords(List<Record<Object>> kafkaRecords, List<ConsumerRecord<String, Object>> partitionRecords) {
        for (ConsumerRecord<String, Object> consumerRecord : partitionRecords) {
            lastReadOffset = kafkaSourceBufferAccumulator.processConsumerRecords(offsetsToCommit, kafkaRecords, lastReadOffset, consumerRecord, partitionRecords);
        }
    }

    private ConsumerRecords<String, Object> poll(final KafkaConsumer<String, Object> consumer) {
        return consumer.poll(Duration.ofMillis(1));
    }

    public void closeConsumer(){
        consumer.close();
    }

    public void shutdownConsumer(){
        consumer.wakeup();
    }
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            consumer.seek(partition, lastReadOffset);
        }
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        try {
            consumer.commitSync(offsetsToCommit);
        } catch (CommitFailedException e) {
            LOG.error("Failed to commit the record for the Json consumer...", e);
        }
    }
}
