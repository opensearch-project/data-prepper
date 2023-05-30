/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.util.KafkaSourceSchemaFactory;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * * A Multithreaded helper class which helps to process the records from multiple topics in an
 * asynchronous way.
 */
@SuppressWarnings("deprecation")
public class MultithreadedConsumer implements Runnable {
	private KafkaConsumer<String, String> plainTextConsumer = null;
	private KafkaConsumer<String, JsonNode> jsonConsumer = null;
	private KafkaConsumer<String, GenericRecord> avroConsumer = null;
	private static final Logger LOG = LoggerFactory.getLogger(MultithreadedConsumer.class);
	private final AtomicBoolean status = new AtomicBoolean(false);
	private final KafkaSourceConfig sourceConfig;
	private final TopicConfig topicConfig;
	private final Buffer<Record<Object>> buffer;
	private String consumerId;
	private String consumerGroupId;
	private String schemaType;
	private Properties consumerProperties;
	private PluginMetrics pluginMetrics;

	public MultithreadedConsumer(String consumerId,
								 String consumerGroupId,
								 Properties properties,
								 TopicConfig topicConfig,
								 KafkaSourceConfig sourceConfig,
								 Buffer<Record<Object>> buffer,
								 PluginMetrics pluginMetric,
								 String schemaType) {
		this.consumerProperties = Objects.requireNonNull(properties);
		this.consumerId = consumerId;
		this.consumerGroupId = consumerGroupId;
		this.sourceConfig = sourceConfig;
		this.topicConfig = topicConfig;
		this.buffer = buffer;
		this.schemaType = schemaType;
		this.pluginMetrics = pluginMetric;
		this.jsonConsumer = new KafkaConsumer<>(consumerProperties);
		this.plainTextConsumer = new KafkaConsumer<>(consumerProperties);
		this.avroConsumer = new KafkaConsumer<>(consumerProperties);

	}

	@SuppressWarnings({ "unchecked" })
	@Override
	public void run() {
		LOG.info("Consumer group : {} and Consumer : {} executed on : {}",consumerGroupId, consumerId,  LocalDateTime.now());
		try {
			MessageFormat schema = MessageFormat.getByMessageFormatByName(schemaType);
			switch(schema){
				case JSON:
					KafkaSourceSchemaFactory.createConsumer(MessageFormat.JSON, jsonConsumer, status, buffer, topicConfig, sourceConfig, schemaType, pluginMetrics).consumeRecords();
					break;
				case AVRO:
					KafkaSourceSchemaFactory.createConsumer(MessageFormat.AVRO, avroConsumer, status, buffer, topicConfig, sourceConfig, schemaType, pluginMetrics).consumeRecords();
					break;
				case PLAINTEXT:
				default:
					KafkaSourceSchemaFactory.createConsumer(MessageFormat.PLAINTEXT, plainTextConsumer, status, buffer, topicConfig, sourceConfig, schemaType, pluginMetrics).consumeRecords();
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
		if (avroConsumer != null) {
			avroConsumer.close();
			avroConsumer = null;
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
		if (avroConsumer != null) {
			avroConsumer.wakeup();
		}
	}
}
