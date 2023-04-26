/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.opensearch.dataprepper.plugins.kafka.source.KafkaSourceBufferAccumulator;
import org.opensearch.dataprepper.plugins.kafka.source.configuration.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * * A helper class which helps to process the JSON records and write them into
 * the buffer. Offset handling and consumer re balance are also being handled
 */

@SuppressWarnings("deprecation")
public class JsonConsumer implements KafkaSchemaTypeConsumer<String, JsonNode>, ConsumerRebalanceListener {
  private static final Logger LOG = LoggerFactory.getLogger(JsonConsumer.class);
  private Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
  private KafkaConsumer<String, JsonNode> kafkaJsonConsumer;
  private long lastReadOffset = 0L;
  private static final Long COMMIT_OFFSET_INTERVAL_IN_MILLI_SEC = 300000L;

  private volatile long lastCommitTime = System.currentTimeMillis();

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void consumeRecords(KafkaConsumer<String, JsonNode> consumer, AtomicBoolean status,
      Buffer<Record<Object>> buffer, TopicConfig topicConfig, PluginMetrics pluginMetrics) {
    KafkaSourceBufferAccumulator kafkaSourceBufferAccumulator = new KafkaSourceBufferAccumulator(topicConfig, pluginMetrics);
    kafkaJsonConsumer = consumer;
    try {
      consumer.subscribe(Arrays.asList(topicConfig.getName()));
      while (!status.get()) {
        ConsumerRecords<String, JsonNode> records = consumer.poll(Duration.ofMillis(100));
        if (!records.isEmpty() && records.count() > 0) {
          for (TopicPartition partition : records.partitions()) {
            List<Record<Object>> kafkaRecords = new ArrayList<>();
            List<ConsumerRecord<String, JsonNode>> partitionRecords = records.records(partition);
            for (ConsumerRecord<String, JsonNode> consumerRecord : partitionRecords) {
              offsetsToCommit.put(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                new OffsetAndMetadata(consumerRecord.offset() + 1, null));
              kafkaRecords.add(kafkaSourceBufferAccumulator.getEventRecord(consumerRecord.value().toString(),topicConfig));
              lastReadOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
            }
            if (!kafkaRecords.isEmpty()){
              kafkaSourceBufferAccumulator.writeAllRecordToBuffer(kafkaRecords,buffer,topicConfig);
            }
          }
          if(!offsetsToCommit.isEmpty()) {
            commitOffsets(consumer);
          }
        }
      }
    } catch (Exception exp) {
      LOG.error("Error while reading the json records from the topic...{}", exp.getMessage());
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

  public synchronized void commitOffsets(KafkaConsumer<String, JsonNode> consumer) {
    try {
      long currentTimeMillis = System.currentTimeMillis();
      if (currentTimeMillis - lastCommitTime > COMMIT_OFFSET_INTERVAL_IN_MILLI_SEC) {
        if(!offsetsToCommit.isEmpty()) {
          consumer.commitSync(offsetsToCommit);
          offsetsToCommit.clear();
          LOG.error("Succeeded to commit the offsets ...");
        }
        lastCommitTime = currentTimeMillis;
      }
    } catch (Exception e) {
      LOG.error("Failed to commit offsets...", e);
    }
  }
}
