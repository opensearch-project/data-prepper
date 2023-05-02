/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source.consumer;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.source.configuration.TopicConfig;

/**
 * * An interface with a generic method which helps to process the records for
 * avro or json or plain text consumer dynamically.
 */
@SuppressWarnings("deprecation")
public interface KafkaSchemaTypeConsumer<K, V> {

  public void consumeRecords(KafkaConsumer<K, V> plainTextConsumer, AtomicBoolean status,
      Buffer<Record<Object>> buffer, TopicConfig sourceConfig, PluginMetrics pluginMetrics);
}
