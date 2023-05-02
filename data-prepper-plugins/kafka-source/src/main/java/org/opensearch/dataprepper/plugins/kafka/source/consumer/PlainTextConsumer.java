/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source.consumer;

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
 * * A helper class which helps to process the Plain Text records and write them
 * into the buffer. Offset handling and consumer re-balancing are also being
 * handled
 */

@SuppressWarnings("deprecation")
public class PlainTextConsumer implements KafkaSchemaTypeConsumer<String, String>, ConsumerRebalanceListener {
  private static final Logger LOG = LoggerFactory.getLogger(PlainTextConsumer.class);
  private Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
  private long lastReadOffset = 0L;
  private KafkaConsumer<String, String> plainTxtConsumer;
  private volatile long lastCommitTime = System.currentTimeMillis();
  private static final Long COMMIT_OFFSET_INTERVAL_MILLISEC = 300000L;

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public void consumeRecords(KafkaConsumer<String, String> consumer, AtomicBoolean status,
      Buffer<Record<Object>> buffer, TopicConfig topicConfig, PluginMetrics pluginMetrics) {
    KafkaSourceBufferAccumulator kafkaSourceBufferAccumulator = new KafkaSourceBufferAccumulator(topicConfig, pluginMetrics);
    plainTxtConsumer = consumer;
    try {
      consumer.subscribe(Arrays.asList(topicConfig.getName()));
      while (!status.get()) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        if (!records.isEmpty() && records.count() > 0) {
          for (TopicPartition partition : records.partitions()) {
            List<Record<Object>> kafkaRecords = new ArrayList<>();
            List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
            for (ConsumerRecord<String, String> consumerRecord : partitionRecords) {
              offsetsToCommit.put(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                 new OffsetAndMetadata(consumerRecord.offset() + 1, null));
              kafkaRecords.add(kafkaSourceBufferAccumulator.getEventRecord(consumerRecord.value(),topicConfig));
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
      LOG.error("Error while reading plain text records from the topic...{}", exp.getMessage());
    }
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    LOG.trace("onPartitionsAssigned() callback triggered and Closing the consumer...");
    for (TopicPartition partition : partitions) {
      plainTxtConsumer.seek(partition, lastReadOffset);
    }
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    LOG.trace("onPartitionsRevoked() callback triggered and Committing the offsets: {} ", offsetsToCommit);
    try {
      plainTxtConsumer.commitSync(offsetsToCommit);
    } catch (CommitFailedException e) {
      LOG.error("Failed to commit the record...", e);
    }
  }

  public void commitOffsets(KafkaConsumer<String, String> consumer) {
    try {
      long currentTimeMillis = System.currentTimeMillis();
      if (currentTimeMillis - lastCommitTime > COMMIT_OFFSET_INTERVAL_MILLISEC) {
        if(!offsetsToCommit.isEmpty()) {
          consumer.commitSync(offsetsToCommit);
          offsetsToCommit.clear();
          LOG.error("Succeeded to commit the offsets ...");
        }
        lastCommitTime = currentTimeMillis;
      }
    } catch (Exception e) {
      LOG.error("Failed to commit the offsets...", e);
    }
  }
}
