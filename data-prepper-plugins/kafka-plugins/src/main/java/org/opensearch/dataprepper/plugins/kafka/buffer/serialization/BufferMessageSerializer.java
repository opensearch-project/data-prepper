/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.buffer.serialization;

import com.google.protobuf.ByteString;
import org.apache.kafka.common.serialization.Serializer;
import org.opensearch.dataprepper.plugins.kafka.buffer.KafkaBufferMessage;

/**
 * A Kafka {@link Serializer} which serializes data into a KafkaBuffer Protobuf message.
 *
 * @param <T> The input type
 */
class BufferMessageSerializer<T> implements Serializer<T> {
    private final Serializer<T> dataSerializer;

    public BufferMessageSerializer(final Serializer<T> dataSerializer) {
        this.dataSerializer = dataSerializer;
    }

    @Override
    public byte[] serialize(final String topic, final T data) {
        final byte[] serializedData = dataSerializer.serialize(topic, data);

        final KafkaBufferMessage.BufferData bufferedData = KafkaBufferMessage.BufferData.newBuilder()
                .setData(ByteString.copyFrom(serializedData))
                .setMessageFormat(KafkaBufferMessage.MessageFormat.MESSAGE_FORMAT_BYTES)
                .build();

        return bufferedData.toByteArray();
    }

    Serializer<T> getDataSerializer() {
        return dataSerializer;
    }
}
