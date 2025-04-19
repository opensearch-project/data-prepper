/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.buffer.serialization;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.opensearch.dataprepper.model.encryption.EncryptionEngine;
import org.opensearch.dataprepper.plugins.encryption.EncryptionSupplier;
import org.opensearch.dataprepper.plugins.kafka.common.KafkaDataConfig;
import org.opensearch.dataprepper.plugins.kafka.common.serialization.SerializationFactory;

import java.util.Objects;

/**
 * An implementation of {@link SerializationFactory} specifically for the Kafka buffer.
 * It makes use the Kafka buffer's Protobuf wrapper.
 */
public class BufferSerializationFactory implements SerializationFactory {
    private final SerializationFactory innerSerializationFactory;
    private final EncryptionSupplier encryptionSupplier;

    /**
     * Create a new instance using an inner {@link SerializationFactory}. You typically pass in
     * the {@link org.opensearch.dataprepper.plugins.kafka.common.serialization.CommonSerializationFactory}.
     *
     * @param innerSerializationFactory The serialization factory for the inner data.
     */
    public BufferSerializationFactory(final SerializationFactory innerSerializationFactory,
                                      final EncryptionSupplier encryptionSupplier) {
        this.innerSerializationFactory = innerSerializationFactory;
        this.encryptionSupplier = encryptionSupplier;
    }

    @Override
    public Deserializer<?> getDeserializer(final KafkaDataConfig dataConfig) {
        final String encryptionId = dataConfig.getEncryptionId();
        final Deserializer<?> dataDeserializer = innerSerializationFactory.getDeserializer(dataConfig);
        if (Objects.isNull(encryptionId)) {
            return new BufferMessageDeserializer<>(dataDeserializer);
        } else {
            final EncryptionEngine encryptionEngine = encryptionSupplier.getEncryptionEngine(encryptionId);
            if (encryptionEngine == null) {
                throw new IllegalArgumentException("Encryption engine not found for encryption_id: " + encryptionId);
            }
            return new BufferMessageEncryptionDeserializer<>(dataDeserializer, encryptionEngine);
        }
    }

    @Override
    public Serializer<?> getSerializer(final KafkaDataConfig dataConfig) {
        final String encryptionId = dataConfig.getEncryptionId();
        final Serializer<?> dataSerializer = innerSerializationFactory.getSerializer(dataConfig);
        if (Objects.isNull(encryptionId)) {
            return new BufferMessageSerializer<>(innerSerializationFactory.getSerializer(dataConfig), dataConfig);
        } else {
            final EncryptionEngine encryptionEngine = encryptionSupplier.getEncryptionEngine(encryptionId);
            if (encryptionEngine == null) {
                throw new IllegalArgumentException("Encryption engine not found for encryption_id: " + encryptionId);
            }
            return new BufferMessageEncryptionSerializer<>(dataSerializer, encryptionEngine);
        }
    }
}
