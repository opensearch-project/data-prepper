/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.kafka.consumer;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.common.TopicPartition;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.buffer.common.BufferAccumulator;
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
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;

/**
 * * A utility class which will handle the core Kafka consumer operation.
 */
public class KafkaSourceCustomConsumer implements Runnable, ConsumerRebalanceListener {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceCustomConsumer.class);
    private Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
    private static final Long COMMIT_OFFSET_INTERVAL_MS = 30000L;
    private static final int DEFAULT_NUMBER_OF_RECORDS_TO_ACCUMULATE = 1;
    private volatile long lastCommitTime;
    private KafkaConsumer consumer= null;
    private AtomicBoolean shutdownInProgress;
    private final String topicName;
    private final TopicConfig topicConfig;
    private PluginMetrics pluginMetrics= null;
    private MessageFormat schema;
    private final BufferAccumulator<Record<Event>> bufferAccumulator;
    private final Buffer<Record<Event>> buffer;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final JsonFactory jsonFactory = new JsonFactory();
    private final Boolean autoCommit;

    public KafkaSourceCustomConsumer(KafkaConsumer consumer,
                                     AtomicBoolean shutdownInProgress,
                                     Buffer<Record<Event>> buffer,
                                     TopicConfig topicConfig,
                                     String schemaType,
                                     Boolean autoCommit,
                                     PluginMetrics pluginMetrics) {
        this.topicName = topicConfig.getName();
        this.topicConfig = topicConfig;
        this.autoCommit = autoCommit;
        this.shutdownInProgress = shutdownInProgress;
        this.consumer = consumer;
        this.buffer = buffer;
        schema = MessageFormat.getByMessageFormatByName(schemaType);
        this.pluginMetrics = pluginMetrics;
        int numberOfRecordsToAccumulate = DEFAULT_NUMBER_OF_RECORDS_TO_ACCUMULATE;
        Duration bufferTimeout = Duration.ofSeconds(1);
        bufferAccumulator = BufferAccumulator.create(buffer, numberOfRecordsToAccumulate, bufferTimeout);
        lastCommitTime = System.currentTimeMillis();
    }

    public <T> void consumeRecords(Map<TopicPartition, OffsetAndMetadata> offsetsToCommit) throws Exception {
        ConsumerRecords<String, T> records =
            consumer.poll(topicConfig.getThreadWaitingTime().toMillis()/2);
        if (!records.isEmpty() && records.count() > 0) {
            Map<TopicPartition, OffsetAndMetadata> offsets = iterateRecordPartitions(records);
            offsets.forEach((partition, offset) ->
                offsetsToCommit.put(partition, offset));
        }
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(Arrays.asList(topicName));
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
            while (!shutdownInProgress.get()) {
                offsetsToCommit.clear();
                consumeRecords(offsetsToCommit);
                long currentTimeMillis = System.currentTimeMillis();
                if (!autoCommit && !offsetsToCommit.isEmpty() &&
                    (currentTimeMillis - lastCommitTime) >= COMMIT_OFFSET_INTERVAL_MS) {
                        try {
                            consumer.commitSync(offsetsToCommit);
                            offsetsToCommit.clear();
                            lastCommitTime = currentTimeMillis;
                        } catch (CommitFailedException e) {
                            LOG.error("Failed to commit offsets in topic "+topicName);
                        }
                }
            }
        } catch (Exception exp) {
            LOG.error("Error while reading the records from the topic...", exp);
        }
    }

    private <T> Record<Event> getRecord(ConsumerRecord<String, T> consumerRecord) {
        Map<String, Object> data = new HashMap<>();
        Event event;
        if (schema == MessageFormat.JSON || schema == MessageFormat.AVRO) {
            Map<String, Object> message = new HashMap<>();
            try {
                final JsonParser jsonParser = jsonFactory.createParser((String)consumerRecord.value().toString());
                message = objectMapper.readValue(jsonParser, Map.class);
            } catch (Exception e){
                LOG.error("Failed to parse JSON or AVRO record");
                return null;
            }
            data.put(consumerRecord.key(), message);
        } else {
            data.put(consumerRecord.key(), (String)consumerRecord.value());
        }
        event = JacksonLog.builder().withData(data).build();
        return new Record<Event>(event);
    }

    private <T> Map<TopicPartition, OffsetAndMetadata> iterateRecordPartitions(ConsumerRecords<String, T> records) throws Exception {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (TopicPartition topicPartition : records.partitions()) {
            List<Record<Event>> kafkaRecords = new ArrayList<>();
            List<ConsumerRecord<String, T>> partitionRecords = records.records(topicPartition);
            for (ConsumerRecord<String, T> consumerRecord : partitionRecords) {
                Record<Event> record = getRecord(consumerRecord);
                if (record != null) {
                    bufferAccumulator.add(record);
                }
            }
            long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
            offsets.put(topicPartition, new OffsetAndMetadata(lastOffset + 1));
        }
        return offsets;
    }

    public void closeConsumer(){
        consumer.close();
    }

    public void shutdownConsumer(){
        consumer.wakeup();
    }
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        for (TopicPartition topicPartition : partitions) {
            Long committedOffset = consumer.committed(topicPartition).offset();
            consumer.seek(topicPartition, committedOffset);
        }
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    }
}
