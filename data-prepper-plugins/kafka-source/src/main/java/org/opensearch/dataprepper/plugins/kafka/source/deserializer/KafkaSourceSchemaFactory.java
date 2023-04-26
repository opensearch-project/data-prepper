/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source.deserializer;

import org.opensearch.dataprepper.plugins.kafka.source.consumer.JsonConsumer;
import org.opensearch.dataprepper.plugins.kafka.source.consumer.KafkaSchemaTypeConsumer;
import org.opensearch.dataprepper.plugins.kafka.source.consumer.PlainTextConsumer;
import org.opensearch.dataprepper.plugins.kafka.source.util.MessageFormat;

/**
 * This is a factory class which will return instance of AVRO,JSON or PLAINTEXT
 * class based on the schema type configured in the pipelines.yaml
 */
public class KafkaSourceSchemaFactory {
  KafkaSourceSchemaFactory() {
  }

  @SuppressWarnings("rawtypes")
  public static KafkaSchemaTypeConsumer getSchemaType(MessageFormat format) {
    switch (format) {
      case JSON:
        return new JsonConsumer();
      case PLAINTEXT:
        return new PlainTextConsumer();
      default:
        throw new IllegalArgumentException("Unknown Schema type consumer other than JSON and PlainText : " + format);
    }
  }
}
