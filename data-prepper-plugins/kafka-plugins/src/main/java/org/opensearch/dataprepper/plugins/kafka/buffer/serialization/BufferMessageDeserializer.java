/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.buffer.serialization;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.common.serialization.Deserializer;
import org.opensearch.dataprepper.plugins.kafka.buffer.KafkaBufferMessage;

/**
 * A Kafka {@link Deserializer} which deserializes data from a KafkaBuffer Protobuf message.
 *
 * @param <T> The output type
 */
class BufferMessageDeserializer<T> implements Deserializer<T> {
    private final Deserializer<T> dataDeserializer;

    public BufferMessageDeserializer(final Deserializer<T> dataDeserializer) {
        this.dataDeserializer = dataDeserializer;
    }

    @Override
    public T deserialize(final String topic, final byte[] data) {
        final KafkaBufferMessage.BufferData bufferedData;
        try {
            bufferedData = KafkaBufferMessage.BufferData.parseFrom(data);
        } catch (final InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }

        final byte[] dataBytes = bufferedData.getData().toByteArray();

        return dataDeserializer.deserialize(topic, dataBytes);
    }

    Deserializer<T> getDataDeserializer() {
        return dataDeserializer;
    }
}
