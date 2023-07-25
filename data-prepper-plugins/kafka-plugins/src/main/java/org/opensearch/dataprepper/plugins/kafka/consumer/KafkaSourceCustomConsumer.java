/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.kafka.consumer;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
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
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
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
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;
import org.apache.commons.lang3.Range;

/**
 * * A utility class which will handle the core Kafka consumer operation.
 */
public class KafkaSourceCustomConsumer implements Runnable, ConsumerRebalanceListener {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceCustomConsumer.class);
    private static final Long COMMIT_OFFSET_INTERVAL_MS = 300000L;
    private static final int DEFAULT_NUMBER_OF_RECORDS_TO_ACCUMULATE = 1;
    static final String POSITIVE_ACKNOWLEDGEMENT_METRIC_NAME = "positiveAcknowledgementSetCounter";
    static final String NEGATIVE_ACKNOWLEDGEMENT_METRIC_NAME = "negativeAcknowledgementSetCounter";
    static final String DEFAULT_KEY = "message";

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
    private Map<TopicPartition, OffsetAndMetadata> offsetsToCommit;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final Map<Integer, TopicPartitionCommitTracker> partitionCommitTrackerMap;
    private final Counter positiveAcknowledgementSetCounter;
    private final Counter negativeAcknowledgementSetCounter;
    private final boolean acknowledgementsEnabled;
    private final Duration acknowledgementsTimeout;

    public KafkaSourceCustomConsumer(final KafkaConsumer consumer,
                                     final AtomicBoolean shutdownInProgress,
                                     final Buffer<Record<Event>> buffer,
                                     final KafkaSourceConfig sourceConfig,
                                     final TopicConfig topicConfig,
                                     final String schemaType,
                                     final AcknowledgementSetManager acknowledgementSetManager,
                                     final PluginMetrics pluginMetrics) {
        this.topicName = topicConfig.getName();
        this.topicConfig = topicConfig;
        this.shutdownInProgress = shutdownInProgress;
        this.consumer = consumer;
        this.buffer = buffer;
        this.offsetsToCommit = new HashMap<>();
        this.acknowledgementsTimeout = sourceConfig.getAcknowledgementsTimeout();
        // If the timeout value is different from default value, then enable acknowledgements automatically.
        this.acknowledgementsEnabled = sourceConfig.getAcknowledgementsEnabled() || acknowledgementsTimeout != KafkaSourceConfig.DEFAULT_ACKNOWLEDGEMENTS_TIMEOUT;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.pluginMetrics = pluginMetrics;
        this.partitionCommitTrackerMap = new HashMap<>();
        this.schema = MessageFormat.getByMessageFormatByName(schemaType);
        Duration bufferTimeout = Duration.ofSeconds(1);
        this.bufferAccumulator = BufferAccumulator.create(buffer, DEFAULT_NUMBER_OF_RECORDS_TO_ACCUMULATE, bufferTimeout);
        this.lastCommitTime = System.currentTimeMillis();
        this.positiveAcknowledgementSetCounter = pluginMetrics.counter(POSITIVE_ACKNOWLEDGEMENT_METRIC_NAME);
        this.negativeAcknowledgementSetCounter = pluginMetrics.counter(NEGATIVE_ACKNOWLEDGEMENT_METRIC_NAME);
    }

    public void updateOffsetsToCommit(final TopicPartition partition, final OffsetAndMetadata offsetAndMetadata) {
        if (Objects.isNull(offsetAndMetadata)) {
            return;
        }
        synchronized (this) {
            offsetsToCommit.put(partition, offsetAndMetadata);
        }
    }

    private AcknowledgementSet createAcknowledgementSet(Map<TopicPartition, Range<Long>> offsets) {
        AcknowledgementSet acknowledgementSet = 
            acknowledgementSetManager.create((result) -> {
                if (result == true) {
                    positiveAcknowledgementSetCounter.increment();
                    offsets.forEach((partition, offsetRange) -> {
                        int partitionId = partition.partition();
                        if (!partitionCommitTrackerMap.containsKey(partitionId)) {
                            OffsetAndMetadata committedOffsetAndMetadata = consumer.committed(partition);
                            Long committedOffset = Objects.nonNull(committedOffsetAndMetadata) ? committedOffsetAndMetadata.offset() : null;
                    
                            partitionCommitTrackerMap.put(partitionId, new TopicPartitionCommitTracker(partition, committedOffset));
                        }
                        OffsetAndMetadata offsetAndMetadata = partitionCommitTrackerMap.get(partitionId).addCompletedOffsets(offsetRange);
                        updateOffsetsToCommit(partition, offsetAndMetadata);
                    });
                } else {
                    positiveAcknowledgementSetCounter.increment();
                }
            }, acknowledgementsTimeout);
        return acknowledgementSet;
    }     

    double getPositiveAcknowledgementsCount() {
        return positiveAcknowledgementSetCounter.count();
    }

    public <T> void consumeRecords() throws Exception {
        ConsumerRecords<String, T> records =
            consumer.poll(topicConfig.getThreadWaitingTime().toMillis()/2);
        if (!records.isEmpty() && records.count() > 0) {
            Map<TopicPartition, Range<Long>> offsets = new HashMap<>();
            AcknowledgementSet acknowledgementSet = null;
            if (acknowledgementsEnabled) {
                acknowledgementSet = createAcknowledgementSet(offsets);
            }
            iterateRecordPartitions(records, acknowledgementSet, offsets);
            if (!acknowledgementsEnabled) {
                offsets.forEach((partition, offsetRange) ->
                    updateOffsetsToCommit(partition, new OffsetAndMetadata(offsetRange.getMaximum() + 1)));
            } else {
                acknowledgementSet.complete();
            }
        }
    }

    private void commitOffsets() {
        if (topicConfig.getAutoCommit()) {
            return;
        }
        long currentTimeMillis = System.currentTimeMillis();
        if ((currentTimeMillis - lastCommitTime) < COMMIT_OFFSET_INTERVAL_MS) {
            return;
        }
        synchronized (this) {
            if (offsetsToCommit.isEmpty()) {
                return;
            }
            try {
                consumer.commitSync();
                offsetsToCommit.clear();
                lastCommitTime = currentTimeMillis;
            } catch (CommitFailedException e) {
                LOG.error("Failed to commit offsets in topic "+topicName);
            }
        }
    }

    Map<TopicPartition, OffsetAndMetadata> getOffsetsToCommit() {
        return offsetsToCommit;
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(Arrays.asList(topicName));
            while (!shutdownInProgress.get()) {
                consumeRecords();
                commitOffsets();
            }
        } catch (Exception exp) {
            LOG.error("Error while reading the records from the topic...", exp);
        }
    }

    private <T> Record<Event> getRecord(ConsumerRecord<String, T> consumerRecord) {
        Map<String, Object> data = new HashMap<>();
        Event event;
        Object value;
        String key = (String)consumerRecord.key();
        if (Objects.isNull(key)) {
            key = DEFAULT_KEY;
        }
        if (schema == MessageFormat.JSON || schema == MessageFormat.AVRO) {
            value = new HashMap<>();
            try {
                if(schema == MessageFormat.JSON){
                    value = consumerRecord.value();
                }else if(schema == MessageFormat.AVRO) {
                    final JsonParser jsonParser = jsonFactory.createParser((String)consumerRecord.value().toString());
                    value = objectMapper.readValue(jsonParser, Map.class);
                }
            } catch (Exception e){
                LOG.error("Failed to parse JSON or AVRO record");
                data.put(key, value);
            }
        } else {
            value = (String)consumerRecord.value();
        }
        data.put(key, value);
        event = JacksonLog.builder().withData(data).build();
        return new Record<Event>(event);
    }

    private <T> void iterateRecordPartitions(ConsumerRecords<String, T> records, final AcknowledgementSet acknowledgementSet, Map<TopicPartition, Range<Long>> offsets) throws Exception {
        for (TopicPartition topicPartition : records.partitions()) {
            List<Record<Event>> kafkaRecords = new ArrayList<>();
            List<ConsumerRecord<String, T>> partitionRecords = records.records(topicPartition);
            for (ConsumerRecord<String, T> consumerRecord : partitionRecords) {
                Record<Event> record = getRecord(consumerRecord);
                if (record != null) {
                    // Always add record to acknowledgementSet before adding to
                    // buffer because another thread may take and process
                    // buffer contents before the event record is added
                    // to acknowledgement set
                    if (acknowledgementSet != null) {
                        acknowledgementSet.add(record.getData());
                    }
                    bufferAccumulator.add(record);
                }
            }
            long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
            long firstOffset = partitionRecords.get(0).offset();
            Range<Long> offsetRange = Range.between(firstOffset, lastOffset);
            offsets.put(topicPartition, offsetRange);
        }
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
