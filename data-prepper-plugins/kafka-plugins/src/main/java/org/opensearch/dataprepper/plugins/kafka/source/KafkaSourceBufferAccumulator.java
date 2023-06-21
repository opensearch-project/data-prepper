/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


/**
 * * A helper utility class which helps to write different formats of records
 * like json, avro and plaintext to the buffer.
 */
@SuppressWarnings("deprecation")
public class KafkaSourceBufferAccumulator<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceBufferAccumulator.class);
    private static final String MESSAGE_KEY = "message";
    private final TopicConfig topicConfig;
    private final KafkaSourceConfig kafkaSourceConfig;
    private final String schemaType;
    private PluginMetrics pluginMetrics;
    private final Counter kafkaConsumerWriteError;
    private static final String KAFKA_CONSUMER_BUFFER_WRITE_ERROR = "kafkaConsumerBufferWriteError";
    private static final int MAX_FLUSH_RETRIES_ON_IO_EXCEPTION = Integer.MAX_VALUE;
    private static final Duration INITIAL_FLUSH_RETRY_DELAY_ON_IO_EXCEPTION = Duration.ofSeconds(5);
    private final JsonFactory jsonFactory = new JsonFactory();
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Long COMMIT_OFFSET_INTERVAL_MILLI_SEC = 300000L;

    public KafkaSourceBufferAccumulator(final TopicConfig topicConfigs,
                                        final KafkaSourceConfig kafkaSourceConfig,
                                        final String schemaType, PluginMetrics pluginMetric) {
        this.kafkaSourceConfig = kafkaSourceConfig;
        this.topicConfig = topicConfigs;
        this.schemaType = schemaType;
        this.pluginMetrics = pluginMetric;
        this.kafkaConsumerWriteError = pluginMetrics.counter(KAFKA_CONSUMER_BUFFER_WRITE_ERROR);
    }

    public Record<Object> getEventRecord(final String line) {
        Map<String, Object> message = new HashMap<>();
        MessageFormat format = MessageFormat.getByMessageFormatByName(schemaType);
        if (format.equals(MessageFormat.JSON) || format.equals(MessageFormat.AVRO)) {
            try {
                final JsonParser jsonParser = jsonFactory.createParser(line);
                message = objectMapper.readValue(jsonParser, Map.class);
            } catch (Exception e) {
                LOG.error("Unable to parse json data [{}]", line, e);
                message.put(MESSAGE_KEY, line);
            }
        } else{
            message.put(MESSAGE_KEY, line);
        }
        Event event = JacksonLog.builder().withData(message).build();
        return new Record<>(event);
    }

    public void write(List<Record<Object>> kafkaRecords, final Buffer<Record<Object>> buffer) throws Exception {
        try {
            writeAllRecordToBuffer(kafkaRecords,
                    buffer, topicConfig);
        } catch (Exception e) {
            if (canRetry(e)) {
                writeWithBackoff(kafkaRecords, buffer, topicConfig);
            }
            LOG.error("Error occurred while writing data to the buffer {}", e.getMessage());
            kafkaConsumerWriteError.increment();
        }
    }

    public synchronized void writeAllRecordToBuffer(List<Record<Object>> kafkaRecords, final Buffer<Record<Object>> buffer, final TopicConfig topicConfig) throws Exception {
        buffer.writeAll(kafkaRecords,
                topicConfig.getBufferDefaultTimeout().toSecondsPart());
    }

    public boolean canRetry(final Exception e) {
        return (e instanceof IOException || e instanceof TimeoutException || e instanceof ExecutionException
                || e instanceof InterruptedException);
    }

    public boolean writeWithBackoff(List<Record<Object>> kafkaRecords, final Buffer<Record<Object>> buffer, final TopicConfig topicConfig) throws Exception {
        final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        long nextDelay = INITIAL_FLUSH_RETRY_DELAY_ON_IO_EXCEPTION.toMillis();
        boolean flushedSuccessfully;

        for (int retryCount = 0; retryCount < MAX_FLUSH_RETRIES_ON_IO_EXCEPTION; retryCount++) {
            final ScheduledFuture<Boolean> flushBufferFuture = scheduledExecutorService.schedule(() -> {
                try {
                    writeAllRecordToBuffer(kafkaRecords, buffer, topicConfig);
                    return true;
                } catch (final TimeoutException e) {
                    return false;
                }
            }, nextDelay, TimeUnit.MILLISECONDS);

            try {
                flushedSuccessfully = flushBufferFuture.get();
                if (flushedSuccessfully) {
                    LOG.info("Successfully flushed the buffer accumulator on retry attempt {}", retryCount + 1);
                    scheduledExecutorService.shutdownNow();
                    return true;
                }
            } catch (final ExecutionException exp) {
                LOG.warn("Retrying of flushing the buffer accumulator hit an exception: {}", exp);
                scheduledExecutorService.shutdownNow();
                throw exp;
            } catch (final InterruptedException exp) {
                LOG.warn("Retrying of flushing the buffer accumulator was interrupted: {}", exp);
                scheduledExecutorService.shutdownNow();
                throw exp;
            }
        }
        LOG.warn("Flushing the bufferAccumulator failed after {} attempts", MAX_FLUSH_RETRIES_ON_IO_EXCEPTION);
        scheduledExecutorService.shutdownNow();
        return false;
    }

    public long commitOffsets(KafkaConsumer<Object, Object> consumer, long lastCommitTime, Map<TopicPartition, OffsetAndMetadata> offsetsToCommit) {
        try {
            long currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis - lastCommitTime > COMMIT_OFFSET_INTERVAL_MILLI_SEC) {
                if(!offsetsToCommit.isEmpty()) {
                    consumer.commitSync(offsetsToCommit);
                    offsetsToCommit.clear();
                    LOG.info("Succeeded to commit the offsets ...");
                }
                lastCommitTime = currentTimeMillis;
            }
        } catch (Exception e) {
            LOG.error("Failed to commit the offsets...", e);
        }
        return lastCommitTime;
    }

    public long processConsumerRecords(Map<TopicPartition, OffsetAndMetadata> offsetsToCommit,
                                       List<Record<Object>> kafkaRecords,
                                       long lastReadOffset, ConsumerRecord<String, String> consumerRecord, List<ConsumerRecord<String, String>> partitionRecords) {
        offsetsToCommit.put(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                new OffsetAndMetadata(consumerRecord.offset() + 1, null));
        kafkaRecords.add(getEventRecord(consumerRecord.value()));
        lastReadOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
        return lastReadOffset;
    }
}
