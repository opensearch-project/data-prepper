/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.consumer;



/**
 * * An interface with a generic method which helps to process the records for
 * avro or json or plain text consumer dynamically.
 */
@SuppressWarnings("deprecation")
public interface KafkaSourceConsumer<K, V> {

  public void consumeRecords();
}
