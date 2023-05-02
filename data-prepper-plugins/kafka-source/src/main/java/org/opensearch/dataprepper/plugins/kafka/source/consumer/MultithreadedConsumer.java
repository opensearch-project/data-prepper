/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source.consumer;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.source.configuration.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * * A helper class which helps to process the records from multiple topics in
 * Multithreaded fashion. Also, it helps to identify the record format is
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


	@Override
	public Object call() throws Exception {
		return null;
	}
}
