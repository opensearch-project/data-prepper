/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.common.serialization;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.opensearch.dataprepper.plugins.kafka.common.KafkaDataConfig;

/**
 * An interface for getting Kafka {@link Deserializer} and {@link Serializer} implementations.
 */
public interface SerializationFactory {
    Deserializer<?> getDeserializer(KafkaDataConfig dataConfig);

    Serializer<?> getSerializer(KafkaDataConfig dataConfig);
}
