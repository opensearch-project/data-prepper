/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.consumer.MultithreadedConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("deprecation")
@DataPrepperPlugin(name = "kafka", pluginType = Source.class, pluginConfigurationType = KafkaSourceConfig.class)
public class KafkaSource implements Source<Record<Object>> {

	private static final Logger logger = LoggerFactory.getLogger(KafkaSource.class);
	private final KafkaSourceConfig sourceConfig;
	private final List<MultithreadedConsumer> consumers = new ArrayList<>();
	private ExecutorService executorService;
	private static final String SCHEMA_TYPE_AVRO = "avro";
	private static final String SCHEMA_TYPE_JSON = "json";

	@DataPrepperPluginConstructor
	public KafkaSource(final KafkaSourceConfig sourceConfig) {
		this.sourceConfig = sourceConfig;
	}

	@Override
	public void start(Buffer<Record<Object>> buffer) {
		int consumerCount = sourceConfig.getConsumerCount();
		String consumerGroups = sourceConfig.getConsumerGroupName();
		Properties props = getConsumerProperties();
		if (!sourceConfig.getTopicName().isEmpty() && isTopicAvailable(sourceConfig.getTopicName(), props)) {
			try {
				if (executorService == null || executorService.isShutdown()) {
					executorService = Executors.newFixedThreadPool(consumerCount);
					IntStream.range(0, consumerCount).forEach(index -> {
						String consumerId = Integer.toString(index + 1);
						MultithreadedConsumer kafkaSourceConsumer = new MultithreadedConsumer(consumerId,
								consumerGroups, props, sourceConfig, buffer);
						consumers.add(kafkaSourceConsumer);
						executorService.submit(kafkaSourceConsumer);
					});
				}
			} catch (Exception e) {
				logger.error("Failed to setup Kafka source.", e);
			}
		} else {
			logger.info("The given TOPIC : {} is not available, hence couldn't be able to read the records...",
					sourceConfig.getTopicName());
		}
	}

	@Override
	public void stop() {
		logger.info("Shutting down Consumers...");
		for (MultithreadedConsumer kafkaConsumer : consumers) {
			kafkaConsumer.shutdownConsumer();
		}
		logger.info("Closing the Kafka Consumer Application...");
		executorService.shutdown();
		try {
			if (!executorService.awaitTermination(sourceConfig.getThreadWaitingTime().toSeconds(),
					TimeUnit.MILLISECONDS)) {
				executorService.shutdownNow();
			}
		} catch (InterruptedException e) {
			if (e.getCause() instanceof InterruptedException) {
				executorService.shutdownNow();
			}
		}
	}

	private Properties getConsumerProperties() {
		Properties props = new Properties();
		try {
			props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
					sourceConfig.getAutoCommitInterval().toSecondsPart());
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, sourceConfig.getAutoOffsetReset());
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, sourceConfig.getBootStrapServers());
			props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, sourceConfig.getEnableAutoCommit());
			props.put(ConsumerConfig.GROUP_ID_CONFIG, sourceConfig.getGroupId());
			props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, sourceConfig.getHeartBeatInterval().toSecondsPart());

			setPropertiesForSchemaType(props, sourceConfig.getSchemaType());
		} catch (Exception e) {
			logger.error("Unable to create Kafka consumer from the given configuration.", e);
		}

		return props;
	}

	private void setPropertiesForSchemaType(Properties props, String schemaType) {

		if (schemaType.equalsIgnoreCase(SCHEMA_TYPE_AVRO)) {
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, sourceConfig.getKeyDeserializer());
		} else if (schemaType.equalsIgnoreCase(SCHEMA_TYPE_JSON)) {
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, sourceConfig.getKeyDeserializer());
		} else {
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
					"org.apache.kafka.common.serialization.StringDeserializer");
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, sourceConfig.getValueDeserializer());
		}
	}

	private boolean isTopicAvailable(String topic, Properties props) {
		AdminClient adminClient = AdminClient.create(props);
		try {
			return adminClient.listTopics().names().get().stream()
					.anyMatch(topicName -> topicName.equalsIgnoreCase(topic));
		} catch (Exception e) {
			logger.error("Error while checking the given topic name : {}", e.getMessage());
			if (e.getCause() instanceof InterruptedException) {
				Thread.currentThread().interrupt();
			}
		} finally {
			adminClient.close();
		}
		return false;
	}
}
