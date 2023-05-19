/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.util;

import org.opensearch.dataprepper.plugins.kafka.consumer.AvroConsumer;
import org.opensearch.dataprepper.plugins.kafka.consumer.JsonConsumer;
import org.opensearch.dataprepper.plugins.kafka.consumer.KafkaSourceSchemaConsumer;
import org.opensearch.dataprepper.plugins.kafka.consumer.PlainTextConsumer;

/**
 * This is a factory class which will return instance of AVRO,JSON or PLAINTEXT
 * class based on the schema type configured in the pipelines.yaml
 */
public class KafkaSourceSchemaFactory {
  KafkaSourceSchemaFactory() {
  }

  @SuppressWarnings("rawtypes")
  public static KafkaSourceSchemaConsumer getSchemaType(MessageFormat format) {
    switch (format) {
      case JSON:
        return new JsonConsumer();
      case AVRO:
        return new AvroConsumer();
      case PLAINTEXT:
        return new PlainTextConsumer();
      default:
        throw new IllegalArgumentException("Unknown Schema type consumer other than JSON and PlainText : " + format);
    }
  }
}
