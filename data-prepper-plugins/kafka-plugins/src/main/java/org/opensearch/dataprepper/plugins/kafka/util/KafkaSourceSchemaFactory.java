/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.util;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.consumer.AvroConsumer;
import org.opensearch.dataprepper.plugins.kafka.consumer.JsonConsumer;
import org.opensearch.dataprepper.plugins.kafka.consumer.KafkaSourceConsumer;
import org.opensearch.dataprepper.plugins.kafka.consumer.PlainTextConsumer;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This is a factory class which will return instance of AVRO,JSON or PLAINTEXT
 * class based on the schema type configured in the pipelines.yaml
 */
public class KafkaSourceSchemaFactory {
  KafkaSourceSchemaFactory() {
  }

  @SuppressWarnings("rawtypes")
  public static KafkaSourceConsumer createConsumer(MessageFormat format,
                                                  KafkaConsumer consumer,
                                                  AtomicBoolean status,
                                                  Buffer<Record<Object>> buffer,
                                                  TopicConfig topicConfig,
                                                  KafkaSourceConfig kafkaSourceConfig,
                                                  String schemaType,
                                                  PluginMetrics pluginMetrics) {
    switch (format) {
      case JSON:
        return new JsonConsumer(consumer, status, buffer, topicConfig, kafkaSourceConfig, schemaType, pluginMetrics);
      case AVRO:
        return new AvroConsumer(consumer, status, buffer, topicConfig, kafkaSourceConfig, schemaType, pluginMetrics);
      case PLAINTEXT:
        return new PlainTextConsumer(consumer, status, buffer, topicConfig, kafkaSourceConfig, schemaType, pluginMetrics);
      default:
        throw new IllegalArgumentException("Unknown Schema type consumer other than JSON, AVRO and PlainText : " + format);
    }
  }
}
