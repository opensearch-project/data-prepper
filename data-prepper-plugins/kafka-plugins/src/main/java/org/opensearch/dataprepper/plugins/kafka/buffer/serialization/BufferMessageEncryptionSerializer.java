/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.buffer.serialization;

import com.google.protobuf.ByteString;
import org.apache.kafka.common.serialization.Serializer;
import org.opensearch.dataprepper.model.encryption.EncryptionEngine;
import org.opensearch.dataprepper.model.encryption.EncryptionEnvelope;
import org.opensearch.dataprepper.plugins.kafka.buffer.KafkaBufferMessage;

import java.util.Objects;

/**
 * A Kafka {@link Serializer} which serializes data into a KafkaBuffer Protobuf message.
 *
 * @param <T> The input type
 */
class BufferMessageEncryptionSerializer<T> implements Serializer<T> {
    private final Serializer<T> dataSerializer;
    private final EncryptionEngine encryptionEngine;

    public BufferMessageEncryptionSerializer(final Serializer<T> dataSerializer,
                                             final EncryptionEngine encryptionEngine) {
        this.dataSerializer = Objects.requireNonNull(dataSerializer);
        this.encryptionEngine = Objects.requireNonNull(encryptionEngine);
    }

    @Override
    public byte[] serialize(final String topic, final T data) {
        if (data == null)
            return null;

        final byte[] serializedData = dataSerializer.serialize(topic, data);
        final EncryptionEnvelope encryptionEnvelope = encryptionEngine.encrypt(serializedData);

        final KafkaBufferMessage.BufferData bufferedData = buildProtobufMessage(encryptionEnvelope);

        return bufferedData.toByteArray();
    }

    Serializer<T> getDataSerializer() {
        return dataSerializer;
    }

    private KafkaBufferMessage.BufferData buildProtobufMessage(final EncryptionEnvelope encryptionEnvelope) {
        return KafkaBufferMessage.BufferData.newBuilder()
                .setEncryptedDataKey(ByteString.copyFromUtf8(encryptionEnvelope.getEncryptedDataKey()))
                .setData(ByteString.copyFrom(encryptionEnvelope.getEncryptedData()))
                .setEncrypted(true)
                .setMessageFormat(KafkaBufferMessage.MessageFormat.MESSAGE_FORMAT_BYTES)
                .build();
    }
}
