/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.buffer.serialization;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.opensearch.dataprepper.plugins.kafka.common.KafkaDataConfig;
import org.opensearch.dataprepper.plugins.kafka.common.serialization.SerializationFactory;

/**
 * An implementation of {@link SerializationFactory} specifically for the Kafka buffer.
 * It makes use the Kafka buffer's Protobuf wrapper.
 */
public class BufferSerializationFactory implements SerializationFactory {
    private final SerializationFactory innerSerializationFactory;

    /**
     * Create a new instance using an inner {@link SerializationFactory}. You typically pass in
     * the {@link org.opensearch.dataprepper.plugins.kafka.common.serialization.CommonSerializationFactory}.
     *
     * @param innerSerializationFactory The serialization factory for the inner data.
     */
    public BufferSerializationFactory(final SerializationFactory innerSerializationFactory) {
        this.innerSerializationFactory = innerSerializationFactory;
    }

    @Override
    public Deserializer<?> getDeserializer(final KafkaDataConfig dataConfig) {
        return new BufferMessageDeserializer<>(innerSerializationFactory.getDeserializer(dataConfig));
    }

    @Override
    public Serializer<?> getSerializer(final KafkaDataConfig dataConfig) {
        return new BufferMessageSerializer<>(innerSerializationFactory.getSerializer(dataConfig));
    }
}
