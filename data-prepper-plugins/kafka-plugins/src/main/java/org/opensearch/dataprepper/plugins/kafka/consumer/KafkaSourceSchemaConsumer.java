/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicsConfig;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * * An interface with a generic method which helps to process the records for
 * avro or json or plain text consumer dynamically.
 */
@SuppressWarnings("deprecation")
public interface KafkaSourceSchemaConsumer<K, V> {

  public void consumeRecords(final KafkaConsumer<K, V> consumer, final AtomicBoolean status,
                             final Buffer<Record<Object>> buffer, final TopicsConfig topicConfig,
                             final KafkaSourceConfig kafkaSourceConfig, PluginMetrics pluginMetrics,
                             final String schemaType);
}
