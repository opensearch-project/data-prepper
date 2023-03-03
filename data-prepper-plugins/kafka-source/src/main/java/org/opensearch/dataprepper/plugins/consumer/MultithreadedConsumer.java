/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.consumer;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.KafkaSourceBuffer;
import org.opensearch.dataprepper.plugins.source.KafkaSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultithreadedConsumer implements Runnable, ConsumerRebalanceListener {

	private final KafkaConsumer<String, String> consumer;
	private KafkaSourceBuffer sourceBuffer;
	private static final Logger logger = LoggerFactory.getLogger(MultithreadedConsumer.class);
	private final AtomicBoolean status = new AtomicBoolean(false);
	private long lastCommitTime = System.currentTimeMillis();
	private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
	private final KafkaSourceConfig sourceConfig;
	@SuppressWarnings("deprecation")
	private final Buffer<Record<Object>> buffer;
	private long lastReadOffset = 0L;
	private int count = 0;
	private String consumerId;
	private String consumerGroupId;
	private Properties consumerProps;
	private boolean firstRetryAttempt = false;

	@SuppressWarnings("deprecation")
	public MultithreadedConsumer(String consumerId, String consumerGroupId, Properties props,
			KafkaSourceConfig sourceConfig, Buffer<Record<Object>> buffer) {
		this.consumerProps = props;
		this.consumer = new KafkaConsumer<>(consumerProps);
		this.consumerId = consumerId;
		this.consumerGroupId = consumerGroupId;
		this.sourceConfig = sourceConfig;
		this.buffer = buffer;
		this.sourceBuffer = new KafkaSourceBuffer(sourceConfig);

	}

	@Override
	public void run() {
		logger.info("Start reading ConsumerId:{}, Consumer group: {}", consumerId, consumerGroupId);
		try {
			consumer.subscribe(Collections.singletonList(sourceConfig.getTopicName()), this);
			while (!status.get()) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				if (records.count() > 0) {
					for (TopicPartition partition : records.partitions()) {
						List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
						for (ConsumerRecord<String, String> consumerRecord : partitionRecords) {
							currentOffsets.put(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
									new OffsetAndMetadata(consumerRecord.offset() + 1, null));
							publishRecordToBuffer(consumerRecord.value(), consumer, partition, consumerRecord,
									lastReadOffset);
						}
						lastReadOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
						commitOffsets(partition, lastReadOffset, consumer);
					}
				}
			}
		} catch (Exception exp) {
			if (exp.getCause() instanceof WakeupException && !status.get()) {
				logger.error("Error reading records from the topic...{}", exp.getMessage());
			}

		} finally {
			logger.info("Closing the consumer... {}", consumerId);
			consumer.close();
		}
	}

	public void shutdownConsumer() {
		status.set(false);
		consumer.wakeup();
	}

	private void publishRecordToBuffer(String recordEvent, KafkaConsumer<String, String> consumer,
			TopicPartition partition, ConsumerRecord<String, String> consumerRecord, long lastReadOffset) {
		try {
			sourceBuffer.writeEventOrStringToBuffer(recordEvent, buffer);
		} catch (Exception exp) {
			logger.error("Error while writing records to the buffer {}", exp.getMessage());
			retryAndProcessFailedRecords(exp, count, lastReadOffset + 1, recordEvent, consumer, partition,
					consumerRecord);
		}
	}

	private void commitOffsets(TopicPartition partition, long lastOffset, KafkaConsumer<String, String> consumer) {
		try {
			long currentTimeMillis = System.currentTimeMillis();
			if (currentTimeMillis - lastCommitTime > sourceConfig.getMaxRecordFetchTime().toSeconds()
					&& sourceConfig.getEnableAutoCommit().equalsIgnoreCase(sourceConfig.getEnableAutoCommit())) {
				consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
			}
			lastCommitTime = currentTimeMillis;
		} catch (CommitFailedException e) {
			logger.error("Failed to commit record. Will try again...", e);
		}
	}

	// development in progress
	private void retryAndProcessFailedRecords(Exception exp, int count, long lastReadOffset, String recordEvent,
			KafkaConsumer<String, String> consumer, TopicPartition partition,
			ConsumerRecord<String, String> consumerRecord) {
		if (exp.getCause() instanceof TimeoutException || exp.getCause() instanceof ExecutionException
				|| exp.getCause() instanceof InterruptedException) {
			if (count < sourceConfig.getMaxRetryAttempts() && !firstRetryAttempt) {
				currentOffsets.put(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
						new OffsetAndMetadata(consumerRecord.offset() + 1, null));
				publishRecordToBuffer(recordEvent, consumer, partition, consumerRecord, lastReadOffset);
				commitOffsets(partition, lastReadOffset, consumer);
			}
		} else if (count < sourceConfig.getMaxRetryAttempts()) {

		} else if (exp.getCause() instanceof IllegalArgumentException
				|| exp.getCause() instanceof NullPointerException) {
			logger.error("Error while retrying to process the record {}", exp.getMessage());
		}
		firstRetryAttempt = true;
	}

	@Override
	public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
		logger.trace("onPartitionsAssigned() callback triggered and Closing the consumer...");
		for (TopicPartition partition : partitions) {
			consumer.seek(partition, lastReadOffset);
		}
	}

	@Override
	public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
		logger.trace("onPartitionsRevoked() callback triggered and Committing the offsets: {} ", currentOffsets);
		// commit offsets for revoked partitions
		try {
			consumer.commitSync(currentOffsets);
		} catch (CommitFailedException e) {
			logger.error("Failed to commit the record...", e);
		}
	}
}
