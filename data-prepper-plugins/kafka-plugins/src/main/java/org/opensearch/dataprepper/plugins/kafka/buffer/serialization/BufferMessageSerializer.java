/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.buffer.serialization;

import com.google.protobuf.ByteString;
import org.apache.kafka.common.serialization.Serializer;
import org.opensearch.dataprepper.plugins.kafka.buffer.KafkaBufferMessage;
import org.opensearch.dataprepper.plugins.kafka.common.KafkaDataConfig;

import java.util.Objects;

/**
 * A Kafka {@link Serializer} which serializes data into a KafkaBuffer Protobuf message.
 *
 * @param <T> The input type
 */
class BufferMessageSerializer<T> implements Serializer<T> {
    private final Serializer<T> dataSerializer;
    private final KafkaDataConfig dataConfig;

    public BufferMessageSerializer(final Serializer<T> dataSerializer, final KafkaDataConfig dataConfig) {
        this.dataSerializer = Objects.requireNonNull(dataSerializer);
        this.dataConfig = Objects.requireNonNull(dataConfig);
    }

    @Override
    public byte[] serialize(final String topic, final T data) {
        if(data == null)
            return null;

        final byte[] serializedData = dataSerializer.serialize(topic, data);

        final KafkaBufferMessage.BufferData bufferedData = buildProtobufMessage(serializedData);

        return bufferedData.toByteArray();
    }

    Serializer<T> getDataSerializer() {
        return dataSerializer;
    }

    private KafkaBufferMessage.BufferData buildProtobufMessage(final byte[] serializedData) {
        KafkaBufferMessage.BufferData.Builder messageBuilder = KafkaBufferMessage.BufferData.newBuilder()
                .setData(ByteString.copyFrom(serializedData))
                .setMessageFormat(KafkaBufferMessage.MessageFormat.MESSAGE_FORMAT_BYTES);

        if(dataConfig.getEncryptionKeySupplier() != null) {
            messageBuilder = messageBuilder.setEncrypted(true);

            if(dataConfig.getEncryptedDataKey() != null)
                messageBuilder = messageBuilder.setEncryptedDataKey(ByteString.copyFromUtf8(dataConfig.getEncryptedDataKey()));
        }

        return messageBuilder.build();
    }
}
