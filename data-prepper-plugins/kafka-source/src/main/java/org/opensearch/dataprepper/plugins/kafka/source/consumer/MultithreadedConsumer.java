/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source.consumer;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.source.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.source.deserializer.KafkaSourceSchemaFactory;
import org.opensearch.dataprepper.plugins.kafka.source.util.MessageFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * * A helper class which helps to process the records from multiple topics in
 * Multithreaded fashion. Also it helps to identify the the record format is
 * json or plaintext dynamically.
 */
@SuppressWarnings("deprecation")
public class MultithreadedConsumer implements Callable<Object> {
	private KafkaConsumer<String, String> plainTextConsumer = null;
	private KafkaConsumer<String, JsonNode> jsonConsumer = null;
	private static final Logger LOG = LoggerFactory.getLogger(MultithreadedConsumer.class);
	private final AtomicBoolean status = new AtomicBoolean(false);


	private final TopicConfig topicConfig;
	private final Buffer<Record<Object>> buffer;
	private String consumerId;
	private String consumerGroupId;
	private Properties consumerProperties;
	private PluginMetrics pluginMetrics;

	public MultithreadedConsumer(String consumerId, String consumerGroupId, Properties properties,
			TopicConfig topicConfig, Buffer<Record<Object>> buffer, PluginMetrics pluginMetric) {
		this.consumerProperties = Objects.requireNonNull(properties);
		this.consumerId = consumerId;
		this.consumerGroupId = consumerGroupId;
		this.topicConfig = topicConfig;
		this.buffer = buffer;
		this.pluginMetrics = pluginMetric;
		this.jsonConsumer = new KafkaConsumer<>(consumerProperties);
		this.plainTextConsumer = new KafkaConsumer<>(consumerProperties);

	}

	@SuppressWarnings({ "unchecked" })
	@Override
	public Object call() {
		LOG.info("Consumer group: {} and Consumer :{} executed on : {}",consumerGroupId, consumerId,  LocalDateTime.now());
		try {
			MessageFormat schemaType = MessageFormat.getByMessageFormatByName(topicConfig.getSchemaConfig().getSchemaType());
			switch(schemaType){
				case JSON:
					KafkaSourceSchemaFactory.getSchemaType(MessageFormat.JSON).consumeRecords(jsonConsumer, status, buffer,
							topicConfig, pluginMetrics);
					break;
				case PLAINTEXT:
				default:
					KafkaSourceSchemaFactory.getSchemaType(MessageFormat.PLAINTEXT).consumeRecords(plainTextConsumer,
							status, buffer, topicConfig, pluginMetrics);
					break;
			}

		} catch (Exception exp) {
			if (exp.getCause() instanceof WakeupException && !status.get()) {
				LOG.error("Error reading records from the topic...{}", exp.getMessage());
			}
		} finally {
			LOG.info("Closing the consumer... {}", consumerId);
			closeConsumers();
		}
		return Thread.currentThread().getName();
	}

	private void closeConsumers() {
		if (plainTextConsumer != null) {
			plainTextConsumer.close();
			plainTextConsumer = null;
		}
		if (jsonConsumer != null) {
			jsonConsumer.close();
			jsonConsumer = null;
		}
	}

	public void shutdownConsumer() {
		status.set(false);
		if (plainTextConsumer != null) {
			plainTextConsumer.wakeup();
		}
		if (jsonConsumer != null) {
			jsonConsumer.wakeup();
		}
	}

}
