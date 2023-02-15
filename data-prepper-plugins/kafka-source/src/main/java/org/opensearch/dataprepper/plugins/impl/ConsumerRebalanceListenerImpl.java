package org.opensearch.dataprepper.plugins.impl;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ConsumerRebalanceListenerImpl implements ConsumerRebalanceListener {

	private static final Logger logger = LoggerFactory.getLogger(ConsumerRebalanceListenerImpl.class);

	private KafkaConsumer<String, String> consumer;
	private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

	public ConsumerRebalanceListenerImpl(KafkaConsumer<String, String> consumer) {
		this.consumer = consumer;
	}

	@Override
	public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
		logger.info("Inside onPartitionsAssigned callback...");
	}

	@Override
	public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
		logger.info("onPartitionsRevoked callback triggered");
		logger.info("Committing offsets: " + currentOffsets);

		consumer.commitSync(currentOffsets);
	}

	public void addOffsetToTrack(String topic, int partition, long offset) {
		currentOffsets.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset + 1, null));
	}

	public Map<TopicPartition, OffsetAndMetadata> getCurrentOffsets() {
		return currentOffsets;
	}
}
