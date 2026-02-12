/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.kafka.consumer;

import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.Range;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.buffer.SizeOverflowException;
import org.opensearch.dataprepper.model.codec.ByteDecoder;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaConsumerConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaKeyMode;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConsumerConfig;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaTopicConsumerMetrics;
import org.opensearch.dataprepper.plugins.kafka.util.LogRateLimiter;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;

/**
 * * A utility class which will handle the core Kafka consumer operation.
 */
public class KafkaCustomConsumer implements Runnable, ConsumerRebalanceListener {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaCustomConsumer.class);
    private static final Long COMMIT_OFFSET_INTERVAL_MS = 300000L;
    private static final int RETRY_ON_EXCEPTION_SLEEP_MS = 1000;
    private static final int BUFFER_WRITE_TIMEOUT = 2000;
    static final String DEFAULT_KEY = "message";

    private volatile long lastCommitTime;
    private KafkaConsumer consumer= null;
    private AtomicBoolean shutdownInProgress;
    private final String topicName;
    private final TopicConsumerConfig topicConfig;
    private MessageFormat schema;
    private boolean paused;
    private final Buffer<Record<Event>> buffer;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final JsonFactory jsonFactory = new JsonFactory();
    private Map<TopicPartition, OffsetAndMetadata> offsetsToCommit;
    private Map<TopicPartition, Long> ownedPartitionsEpoch;
    private Set<TopicPartition> partitionsToReset;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final Map<Integer, TopicPartitionCommitTracker> partitionCommitTrackerMap;
    private List<Map<TopicPartition, CommitOffsetRange>> acknowledgedOffsets;
    private final boolean acknowledgementsEnabled;
    private final Duration acknowledgementsTimeout;
    private final KafkaTopicConsumerMetrics topicMetrics;
    private final PauseConsumePredicate pauseConsumePredicate;
    private long metricsUpdatedTime;
    private final AtomicInteger numberOfAcksPending;
    private long numRecordsCommitted = 0;
    private final LogRateLimiter errLogRateLimiter;
    private final ByteDecoder byteDecoder;
    private final long maxRetriesOnException;
    private final Map<Integer, Long> partitionToLastReceivedTimestampMillis;
    private final CompressionOption compressionConfig;

    public KafkaCustomConsumer(final KafkaConsumer consumer,
                               final AtomicBoolean shutdownInProgress,
                               final Buffer<Record<Event>> buffer,
                               final KafkaConsumerConfig consumerConfig,
                               final TopicConsumerConfig topicConfig,
                               final String schemaType,
                               final AcknowledgementSetManager acknowledgementSetManager,
                               final ByteDecoder byteDecoder,
                               final KafkaTopicConsumerMetrics topicMetrics,
                               final PauseConsumePredicate pauseConsumePredicate,
                               final CompressionOption compressionConfig) {
        this.topicName = topicConfig.getName();
        this.topicConfig = topicConfig;
        this.shutdownInProgress = shutdownInProgress;
        this.consumer = consumer;
        this.buffer = buffer;
        this.paused = false;
        this.byteDecoder = byteDecoder;
        this.topicMetrics = topicMetrics;
        this.maxRetriesOnException = topicConfig.getMaxPollInterval().toMillis() / (2 * (RETRY_ON_EXCEPTION_SLEEP_MS + BUFFER_WRITE_TIMEOUT));
        this.pauseConsumePredicate = pauseConsumePredicate;
        this.topicMetrics.register(consumer);
        this.offsetsToCommit = new HashMap<>();
        this.partitionToLastReceivedTimestampMillis = new HashMap<>();
        this.ownedPartitionsEpoch = new HashMap<>();
        this.metricsUpdatedTime = Instant.now().getEpochSecond();
        this.acknowledgedOffsets = new ArrayList<>();
        this.acknowledgementsTimeout = Duration.ofSeconds(Integer.MAX_VALUE);
        this.acknowledgementsEnabled = consumerConfig.getAcknowledgementsEnabled();
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.partitionCommitTrackerMap = new HashMap<>();
        this.partitionsToReset = Collections.synchronizedSet(new HashSet<>());
        this.schema = MessageFormat.getByMessageFormatByName(schemaType);
        this.lastCommitTime = System.currentTimeMillis();
        this.numberOfAcksPending = new AtomicInteger(0);
        this.errLogRateLimiter = new LogRateLimiter(2, System.currentTimeMillis());
        this.compressionConfig = (compressionConfig == null) ? CompressionOption.NONE : compressionConfig;
    }

    public KafkaCustomConsumer(final KafkaConsumer consumer,
                               final AtomicBoolean shutdownInProgress,
                               final Buffer<Record<Event>> buffer,
                               final KafkaConsumerConfig consumerConfig,
                               final TopicConsumerConfig topicConfig,
                               final String schemaType,
                               final AcknowledgementSetManager acknowledgementSetManager,
                               final ByteDecoder byteDecoder,
                               final KafkaTopicConsumerMetrics topicMetrics,
                               final PauseConsumePredicate pauseConsumePredicate) {
        this(consumer, shutdownInProgress, buffer, consumerConfig, topicConfig, schemaType, acknowledgementSetManager, byteDecoder, topicMetrics, pauseConsumePredicate, CompressionOption.NONE);
    }

    KafkaTopicConsumerMetrics getTopicMetrics() {
        return topicMetrics;
    }

    <T> long getRecordTimeStamp(final ConsumerRecord<String, T> consumerRecord, final long nowMs) {
        final long timestamp = consumerRecord.timestamp();
        int partition = consumerRecord.partition();
        if (timestamp > nowMs) {
            topicMetrics.getNumberOfInvalidTimeStamps().increment();
            if (partitionToLastReceivedTimestampMillis.containsKey(partition)) {
                return partitionToLastReceivedTimestampMillis.get(partition);
            } else {
                return nowMs;
            }
        } else {
            partitionToLastReceivedTimestampMillis.put(partition, timestamp);
            return timestamp;
        }
    }

    private long getCurrentTimeNanos() {
        Instant now = Instant.now();
        return now.getEpochSecond()*1000000000+now.getNano();
    }

    public void updateOffsetsToCommit(final TopicPartition partition, final OffsetAndMetadata offsetAndMetadata) {
        if (Objects.isNull(offsetAndMetadata)) {
            return;
        }
        synchronized (offsetsToCommit) {
            offsetsToCommit.put(partition, offsetAndMetadata);
        }
    }

    private AcknowledgementSet createAcknowledgementSet(Map<TopicPartition, CommitOffsetRange> offsets) {
        AcknowledgementSet acknowledgementSet =
                acknowledgementSetManager.create((result) -> {
                    numberOfAcksPending.decrementAndGet();
                    if (result == true) {
                        topicMetrics.getNumberOfPositiveAcknowledgements().increment();
                        synchronized(this) {
                            acknowledgedOffsets.add(offsets);
                        }
                    } else {
                        topicMetrics.getNumberOfNegativeAcknowledgements().increment();
                        synchronized(this) {
                            offsets.forEach((partition, offsetRange) -> {
                                partitionsToReset.add(partition);
                            });
                        }
                    }
                }, acknowledgementsTimeout);
        return acknowledgementSet;
    }

    <T> ConsumerRecords<String, T> doPoll() throws Exception {
            topicMetrics.recordTimeBetweenPolls();
            ConsumerRecords<String, T> records =
                    consumer.poll(Duration.ofMillis(topicConfig.getThreadWaitingTime().toMillis()/2));
            return records;
    }

    <T> void consumeRecords() throws Exception {
        try {
            ConsumerRecords<String, T> records = doPoll();
            LOG.debug("Consumed records with count {}", records.count());
            if (Objects.nonNull(records) && !records.isEmpty() && records.count() > 0) {
                Map<TopicPartition, CommitOffsetRange> offsets = new HashMap<>();
                AcknowledgementSet acknowledgementSet = null;
                if (acknowledgementsEnabled) {
                    acknowledgementSet = createAcknowledgementSet(offsets);
                }
                iterateRecordPartitions(records, acknowledgementSet, offsets);
                if (!acknowledgementsEnabled) {
                    offsets.forEach((partition, offsetRange) -> {
                        updateOffsetsToCommit(partition, new OffsetAndMetadata(offsetRange.getOffsets().getMaximum() + 1));
                        numRecordsCommitted += offsetRange.getOffsets().getMaximum() - offsetRange.getOffsets().getMinimum() + 1;
                    });
                } else {
                    acknowledgementSet.complete();
                    numberOfAcksPending.incrementAndGet();
                }
            }
        } catch (AuthenticationException e) {
            LOG.warn("Authentication error while doing poll(). Will retry after 10 seconds", e);
            topicMetrics.getNumberOfPollAuthErrors().increment();
            Thread.sleep(10000);
        } catch (RecordDeserializationException e) {
            LOG.warn("Deserialization error - topic {} partition {} offset {}. Error message: {}",
                    e.topicPartition().topic(), e.topicPartition().partition(), e.offset(), e.getMessage());
            if (getRootCause(e) instanceof AccessDeniedException) {
                LOG.warn("AccessDenied for AWSGlue schema registry, retrying after 30 seconds");
                Thread.sleep(30000);
            } else {
                LOG.warn("Seeking past the error record");
                consumer.seek(e.topicPartition(), e.offset() + 1);

                // Update failed record offset in commitTracker because we are not
                // processing it and seeking past the error record.
                // Note: partitionCommitTrackerMap may not have the partition if this is
                // ths first set of records that hit deserialization error
                if (acknowledgementsEnabled && partitionCommitTrackerMap.containsKey(e.topicPartition().partition())) {
                    addAcknowledgedOffsets(e.topicPartition(), Range.of(e.offset(), e.offset()));
                }
            }

            topicMetrics.getNumberOfDeserializationErrors().increment();
        }
    }

    private void addAcknowledgedOffsets(final TopicPartition topicPartition, final Range<Long> offsetRange) {
        final int partitionId = topicPartition.partition();
        final TopicPartitionCommitTracker commitTracker = partitionCommitTrackerMap.get(partitionId);

        if (Objects.isNull(commitTracker) && errLogRateLimiter.isAllowed(System.currentTimeMillis())) {
            LOG.error("Commit tracker not found for TopicPartition: {}", topicPartition);
            return;
        }

        final OffsetAndMetadata offsetAndMetadata = commitTracker.addCompletedOffsets(offsetRange);
        updateOffsetsToCommit(topicPartition, offsetAndMetadata);
    }

    private void resetOffsets() {
        // resetting offsets is similar to committing acknowledged offsets. Throttle the frequency of resets by
        // checking current time with last commit time. Same "lastCommitTime" and commit interval are used in both cases
        long currentTimeMillis = System.currentTimeMillis();
        if ((currentTimeMillis - lastCommitTime) < topicConfig.getCommitInterval().toMillis()) {
            return;
        }
        if (partitionsToReset.size() > 0) {
            partitionsToReset.forEach(partition -> {
                try {
                    final OffsetAndMetadata offsetAndMetadata = consumer.committed(partition);
                    if (Objects.isNull(offsetAndMetadata)) {
                        LOG.info("Seeking partition {} to the beginning", partition);
                        consumer.seekToBeginning(List.of(partition));
                    } else {
                        LOG.info("Seeking partition {} to {}", partition, offsetAndMetadata.offset());
                        consumer.seek(partition, offsetAndMetadata);
                    }
                    partitionCommitTrackerMap.remove(partition.partition());
                    final long epoch = getCurrentTimeNanos();
                    ownedPartitionsEpoch.put(partition, epoch);
                } catch (Exception e) {
                    LOG.error("Failed to seek to last committed offset upon negative acknowledgement {}", partition, e);
                }
            });
            partitionsToReset.clear();
        }
    }

    void processAcknowledgedOffsets() {
        acknowledgedOffsets.forEach(offsets -> {
            offsets.forEach((partition, offsetRange) -> {
                if (getPartitionEpoch(partition) == offsetRange.getEpoch()) {
                    try {
                        addAcknowledgedOffsets(partition, offsetRange.getOffsets());
                    } catch (Exception e) {
                        LOG.error("Failed committed offsets upon positive acknowledgement {}", partition, e);
                    }
                }
            });
        });
        acknowledgedOffsets.clear();
    }

    private void updateCommitCountMetric(final TopicPartition topicPartition, final OffsetAndMetadata offsetAndMetadata) {
        if (acknowledgementsEnabled) {
            final TopicPartitionCommitTracker commitTracker = partitionCommitTrackerMap.get(topicPartition.partition());
            if (Objects.isNull(commitTracker) && errLogRateLimiter.isAllowed(System.currentTimeMillis())) {
                LOG.error("Commit tracker not found for TopicPartition: {}", topicPartition);
                return;
            }
            topicMetrics.getNumberOfRecordsCommitted().increment(commitTracker.getCommittedRecordCount());
        } else {
            topicMetrics.getNumberOfRecordsCommitted().increment(numRecordsCommitted);
            numRecordsCommitted = 0;
        }
    }

    private void commitOffsets(boolean forceCommit) {
        if (topicConfig.getAutoCommit()) {
            return;
        }
        processAcknowledgedOffsets();
        long currentTimeMillis = System.currentTimeMillis();
        if (!forceCommit && (currentTimeMillis - lastCommitTime) < topicConfig.getCommitInterval().toMillis()) {
            return;
        }
        synchronized (offsetsToCommit) {
            if (offsetsToCommit.isEmpty()) {
                return;
            }

            offsetsToCommit.forEach(((partition, offset) -> updateCommitCountMetric(partition, offset)));
            try {
                consumer.commitSync(offsetsToCommit);
                lastCommitTime = currentTimeMillis;
            } catch (final RebalanceInProgressException ex) {
                LOG.error("Failed to commit offsets in topic {} due to rebalance in progress", topicName, ex);
                return;
            } catch (Exception e) {
                LOG.error("Failed to commit offsets in topic {}", topicName, e);
            }

            offsetsToCommit.clear();
        }
    }

    @VisibleForTesting
    Map<TopicPartition, OffsetAndMetadata> getOffsetsToCommit() {
        return offsetsToCommit;
    }

    @VisibleForTesting
    Long getNumRecordsCommitted() {
        return numRecordsCommitted;
    }

    @Override
    public void run() {
        consumer.subscribe(Arrays.asList(topicName), this);
        Set<TopicPartition> partitions = consumer.assignment();
        final long currentEpoch = getCurrentTimeNanos();
        partitions.forEach((partition) -> {
            final OffsetAndMetadata offsetAndMetadata = consumer.committed(partition);
            LOG.info("Starting consumer with topic partition ({}) offset {}", partition, offsetAndMetadata);
            ownedPartitionsEpoch.put(partition, currentEpoch);
        });

        boolean retryingAfterException = false;
        while (!shutdownInProgress.get()) {
            LOG.debug("Still running Kafka consumer in start of loop");
            try {
                if (retryingAfterException) {
                    LOG.debug("Pause consuming from Kafka topic due a previous exception.");
                    Thread.sleep(10000);
                } else if (pauseConsumePredicate.pauseConsuming()) {
                    LOG.debug("Pause and skip consuming from Kafka topic due to an external condition: {}", pauseConsumePredicate);
                    paused = true;
                    consumer.pause(consumer.assignment());
                    Thread.sleep(1000);
                    continue;
                } else if(paused) {
                    LOG.debug("Resume consuming from Kafka topic.");
                    paused = false;
                    consumer.resume(consumer.assignment());
                }
                LOG.debug("Still running Kafka consumer preparing to commit offsets and consume records");
                synchronized(this) {
                    commitOffsets(false);
                    resetOffsets();
                }
                consumeRecords();
                LOG.debug("Exited consume records");
                topicMetrics.update(consumer);
                LOG.debug("Updated consumer metrics");
                retryingAfterException = false;
            } catch (Exception exp) {
                LOG.error("Error while reading the records from the topic {}. Retry after 10 seconds", topicName, exp);
                retryingAfterException = true;
            }
        }
        LOG.info("Shutting down, number of acks pending = {}", numberOfAcksPending.get());
        synchronized(this) {
            commitOffsets(true);
        }
    }

    private <T> Record<Event> getRecord(ConsumerRecord<String, T> consumerRecord, int partition) {
        Instant now = Instant.now();
        Map<String, Object> data = new HashMap<>();
        Event event;
        Object value = consumerRecord.value();
        String key = (String)consumerRecord.key();
        KafkaKeyMode kafkaKeyMode = topicConfig.getKafkaKeyMode();
        boolean plainTextMode = false;
        try {
            if (value instanceof JsonDataWithSchema) {
                JsonDataWithSchema j = (JsonDataWithSchema)consumerRecord.value();
                value = objectMapper.readValue(j.getPayload(), Map.class);
            } else if (schema == MessageFormat.AVRO || value instanceof GenericRecord) {
                final JsonParser jsonParser = jsonFactory.createParser((String)consumerRecord.value().toString());
                value = objectMapper.readValue(jsonParser, Map.class);
            } else if (schema == MessageFormat.PLAINTEXT) {
                value = (String)consumerRecord.value();
                plainTextMode = true;
            } else if (schema == MessageFormat.JSON) {
                value = objectMapper.convertValue(value, Map.class);
            }
        } catch (Exception e){
            LOG.error("Failed to parse JSON or AVRO record", e);
            topicMetrics.getNumberOfRecordsFailedToParse().increment();
        }
        if (!plainTextMode) {
            if (!(value instanceof Map)) {
                data.put(key, value);
            } else {
                Map<String, Object> valueMap = (Map<String, Object>)value;
                if (kafkaKeyMode == KafkaKeyMode.INCLUDE_AS_FIELD) {
                    valueMap.put("kafka_key", key);
                }
                data = valueMap;
            }
        } else {
            if (Objects.isNull(key)) {
                key = DEFAULT_KEY;
            }
            data.put(key, value);
        }
        event = JacksonLog.builder().withData(data).build();
        EventMetadata eventMetadata = event.getMetadata();
        if (kafkaKeyMode == KafkaKeyMode.INCLUDE_AS_METADATA) {
            eventMetadata.setAttribute("kafka_key", key);
        }
        Headers headers = consumerRecord.headers();
        if (headers != null) {
            Map<String, Object> headerData = new HashMap<>();
            for (Header header: headers) {
                byte[] headerValue = header.value();
                try {
                    headerData.put(header.key(), ((headerValue != null) ? new String(header.value(), StandardCharsets.UTF_8) : null));
                } catch (Exception e) {
                    headerData.put(header.key(), headerValue);
                }
            }
            eventMetadata.setAttribute("kafka_headers", headerData);
        }
        final long receivedTimeStamp = getRecordTimeStamp(consumerRecord, now.toEpochMilli());
        eventMetadata.setAttribute("kafka_timestamp", receivedTimeStamp);
        eventMetadata.setAttribute("kafka_timestamp_type", consumerRecord.timestampType().toString());
        eventMetadata.setAttribute("kafka_topic", topicName);
        eventMetadata.setAttribute("kafka_partition", String.valueOf(partition));
        eventMetadata.setExternalOriginationTime(Instant.ofEpochMilli(receivedTimeStamp));
        event.getEventHandle().setExternalOriginationTime(Instant.ofEpochMilli(receivedTimeStamp));

        return new Record<Event>(event);
    }

    private void processRecords(final AcknowledgementSet acknowledgementSet, final List<Record<Event>> eventRecords) {
        // Always add record to acknowledgementSet before adding to
        // buffer because another thread may take and process
        // buffer contents before the event record is added
        // to acknowledgement set
        if (acknowledgementSet != null) {
            eventRecords.forEach(record -> acknowledgementSet.add(record.getData()));
        }
        long numRetries = 0;
        while (true) {
            LOG.debug("In while loop for processing records, paused = {}", paused);
            try {
                buffer.writeAll(eventRecords, BUFFER_WRITE_TIMEOUT);
                break;
            } catch (Exception e) {
                if (!paused && numRetries++ > maxRetriesOnException) {
                    paused = true;
                    LOG.debug("Preparing to call pause");
                    consumer.pause(consumer.assignment());
                    LOG.debug("Pause was called");
                }
                if (e instanceof SizeOverflowException) {
                    topicMetrics.getNumberOfBufferSizeOverflows().increment();
                } else {
                    LOG.debug("Error while adding record to buffer, retrying ", e);
                }
                try {
                    LOG.debug("Sleeping due to exception");
                    Thread.sleep(RETRY_ON_EXCEPTION_SLEEP_MS);
                    if (paused) {
                        LOG.debug("Calling doPoll()");
                        ConsumerRecords<String, ?> records = doPoll();
                        if (records.count() > 0) {
                            LOG.warn("Unexpected records received while the consumer is paused. Resetting the partitions to retry from last read pointer");
                            synchronized(this) {
                                partitionsToReset.addAll(consumer.assignment());
                            };
                            break;
                        }
                    }
                } catch (Exception ex) {} // ignore the exception because it only means the thread slept for shorter time
            }
        }

        if (paused) {
            LOG.debug("Resuming consumption");
            consumer.resume(consumer.assignment());
            paused = false;
        }
    }

    private <T> void iterateRecordPartitions(ConsumerRecords<String, T> records, final AcknowledgementSet acknowledgementSet,
                                             Map<TopicPartition, CommitOffsetRange> offsets) throws Exception {
        for (TopicPartition topicPartition : records.partitions()) {
            final long partitionEpoch = getPartitionEpoch(topicPartition);
            if (acknowledgementsEnabled && partitionEpoch == 0) {
                if (errLogRateLimiter.isAllowed(System.currentTimeMillis())) {
                    LOG.error("Lost ownership of partition {}", topicPartition);
                }
                continue;
            }

            List<ConsumerRecord<String, T>> partitionRecords = records.records(topicPartition);
            final List<Record<Event>> eventRecords = new ArrayList<>();
            for (ConsumerRecord<String, T> consumerRecord : partitionRecords) {
                if (schema == MessageFormat.BYTES) {
                    InputStream byteInputStream = new ByteArrayInputStream((byte[])consumerRecord.value());
                    InputStream decompressedInputStream = compressionConfig.getDecompressionEngine().createInputStream(byteInputStream);

                    if(byteDecoder != null) {
                        final long receivedTimeStamp = getRecordTimeStamp(consumerRecord, Instant.now().toEpochMilli());

                        byteDecoder.parse(decompressedInputStream, Instant.ofEpochMilli(receivedTimeStamp), eventRecords::add);
                    } else {
                        JsonNode jsonNode = objectMapper.readValue(decompressedInputStream, JsonNode.class);

                        Event event = JacksonLog.builder().withData(jsonNode).build();
                        Record<Event> record = new Record<>(event);
                        eventRecords.add(record);
                    }
                } else {
                    Record<Event> record = getRecord(consumerRecord, topicPartition.partition());
                    if (record != null) {
                        eventRecords.add(record);
                    }
                }
            }

            processRecords(acknowledgementSet, eventRecords);

            long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
            long firstOffset = partitionRecords.get(0).offset();
            Range<Long> offsetRange = Range.between(firstOffset, lastOffset);
            offsets.put(topicPartition, new CommitOffsetRange(offsetRange, partitionEpoch));

            if (acknowledgementsEnabled && !partitionCommitTrackerMap.containsKey(topicPartition.partition())) {
                partitionCommitTrackerMap.put(topicPartition.partition(),
                        new TopicPartitionCommitTracker(topicPartition, firstOffset));
            }
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
        synchronized(this) {
            final long epoch = getCurrentTimeNanos();

            for (TopicPartition topicPartition : partitions) {
                if (ownedPartitionsEpoch.containsKey(topicPartition)) {
                    LOG.info("Partition {} already owned", topicPartition);
                    continue;
                }
                LOG.info("Assigned partition {}", topicPartition);
                ownedPartitionsEpoch.put(topicPartition, epoch);
            }
            if (paused) {
                consumer.pause(consumer.assignment());
            }
        }
        dumpTopicPartitionOffsets(partitions);
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        synchronized(this) {
            commitOffsets(true);
            for (TopicPartition topicPartition : partitions) {
                if (!ownedPartitionsEpoch.containsKey(topicPartition)) {
                    LOG.info("Partition {} not owned", topicPartition);
                    continue;
                }
                LOG.info("Revoked partition {}", topicPartition);
                ownedPartitionsEpoch.remove(topicPartition);
                partitionCommitTrackerMap.remove(topicPartition.partition());
            }
            if (paused) {
                consumer.pause(consumer.assignment());
            }
        }
    }

    private long getPartitionEpoch(final TopicPartition topicPartition) {
        return ownedPartitionsEpoch.getOrDefault(topicPartition, 0L);
    }

    final void dumpTopicPartitionOffsets(final Collection<TopicPartition> partitions) {
        try {
            final Map<TopicPartition, OffsetAndMetadata> committedOffsets = consumer.committed(new HashSet<>(partitions));
            final Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);
            final Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
            for (TopicPartition topicPartition : partitions) {
                final OffsetAndMetadata offsetAndMetadata = committedOffsets.get(topicPartition);
                LOG.info("Partition {} offsets: beginningOffset: {}, endOffset: {}, committedOffset: {}",
                        topicPartition, getTopicPartitionOffset(beginningOffsets, topicPartition),
                        getTopicPartitionOffset(endOffsets, topicPartition),
                        Objects.isNull(offsetAndMetadata) ? "-" : offsetAndMetadata.offset());
            }
        } catch (Exception e) {
            LOG.error("Failed to get offsets in onPartitionsAssigned callback", e);
        }
    }

    final String getTopicPartitionOffset(final Map<TopicPartition, Long> offsetMap, final TopicPartition topicPartition) {
        final Long offset = offsetMap.get(topicPartition);
        return Objects.isNull(offset) ? "-" : offset.toString();
    }
}
