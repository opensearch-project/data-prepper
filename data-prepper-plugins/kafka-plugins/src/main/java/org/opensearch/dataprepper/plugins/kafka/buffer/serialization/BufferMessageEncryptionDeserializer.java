/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.buffer.serialization;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.common.serialization.Deserializer;
import org.opensearch.dataprepper.model.encryption.EncryptionEngine;
import org.opensearch.dataprepper.plugins.encryption.DefaultEncryptionEnvelope;
import org.opensearch.dataprepper.plugins.kafka.buffer.KafkaBufferMessage;

/**
 * A Kafka {@link Deserializer} which deserializes data from a KafkaBuffer Protobuf message.
 *
 * @param <T> The output type
 */
class BufferMessageEncryptionDeserializer<T> implements Deserializer<T> {
    private final Deserializer<T> dataDeserializer;
    private final EncryptionEngine encryptionEngine;

    public BufferMessageEncryptionDeserializer(final Deserializer<T> dataDeserializer,
                                               final EncryptionEngine encryptionEngine) {
        this.dataDeserializer = dataDeserializer;
        this.encryptionEngine = encryptionEngine;
    }

    @Override
    public T deserialize(final String topic, final byte[] data) {
        final KafkaBufferMessage.BufferData bufferedData;
        try {
            bufferedData = KafkaBufferMessage.BufferData.parseFrom(data);
        } catch (final InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }

        final byte[] dataBytes = encryptionEngine.decrypt(
                DefaultEncryptionEnvelope.builder()
                        .encryptedDataKey(bufferedData.getEncryptedDataKey().toStringUtf8())
                        .encryptedData(bufferedData.getData().toByteArray())
                        .build());

        return dataDeserializer.deserialize(topic, dataBytes);
    }

    Deserializer<T> getDataDeserializer() {
        return dataDeserializer;
    }
}
