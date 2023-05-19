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
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicsConfig;
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
public class JsonConsumer implements KafkaSourceSchemaConsumer<String, JsonNode>, ConsumerRebalanceListener {
    private static final Logger LOG = LoggerFactory.getLogger(JsonConsumer.class);
    private Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
    private KafkaConsumer<String, JsonNode> kafkaJsonConsumer;
    private long lastReadOffset = 0L;
    private volatile long lastCommitTime = System.currentTimeMillis();

    @SuppressWarnings({"rawtypes", "unchecked"})
    public void consumeRecords(final KafkaConsumer<String, JsonNode> consumer, final AtomicBoolean status,
                               final Buffer<Record<Object>> buffer, final TopicsConfig topicConfig,
                               final KafkaSourceConfig kafkaSourceConfig, PluginMetrics pluginMetrics,
                               final String schemaType) {
        KafkaSourceBufferAccumulator kafkaSourceBufferAccumulator = new KafkaSourceBufferAccumulator(topicConfig,kafkaSourceConfig,
                schemaType,pluginMetrics);
        kafkaJsonConsumer = consumer;
        try {
            consumer.subscribe(Arrays.asList(topicConfig.getName()));
            while (!status.get()) {
                offsetsToCommit.clear();
                ConsumerRecords<String, JsonNode> records = consumer.poll(Duration.ofMillis(100));
                if (!records.isEmpty() && records.count() > 0) {
                    for (TopicPartition partition : records.partitions()) {
                        List<Record<Object>> kafkaRecords = new ArrayList<>();
                        List<ConsumerRecord<String, JsonNode>> partitionRecords = records.records(partition);
                        for (ConsumerRecord<String, JsonNode> consumerRecord : partitionRecords) {
                            offsetsToCommit.put(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                                    new OffsetAndMetadata(consumerRecord.offset() + 1, null));
                            kafkaRecords.add(kafkaSourceBufferAccumulator.getEventRecord(consumerRecord.value().toString()));
                            lastReadOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                        }
                        if (!kafkaRecords.isEmpty()) {
                            kafkaSourceBufferAccumulator.writeAllRecordToBuffer(kafkaRecords, buffer, topicConfig);
                        }
                    }
                    if (!offsetsToCommit.isEmpty() && topicConfig.getAutoCommit().equalsIgnoreCase("false")) {
                        lastCommitTime = kafkaSourceBufferAccumulator.commitOffsets(consumer, lastCommitTime, offsetsToCommit);
                    }
                }
            }
        } catch (Exception exp) {
            LOG.error("Error while reading the json records from the topic...", exp);
        }
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
