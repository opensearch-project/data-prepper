/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.buffer.serialization;

import com.google.protobuf.ByteString;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.kafka.buffer.KafkaBufferMessage;

import java.util.Random;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BufferMessageDeserializerTest {
    @Mock
    private Deserializer<Object> innerDataDeserializer;

    private Random random;
    private String topic;

    @BeforeEach
    void setUp() {
        random = new Random();
        topic = UUID.randomUUID().toString();
    }

    private BufferMessageDeserializer<Object> createObjectUnderTest() {
        return new BufferMessageDeserializer<>(innerDataDeserializer);
    }

    @Test
    void getDataDeserializer_returns_innerDataDeserializer_to_help_with_other_tests() {
        assertThat(createObjectUnderTest().getDataDeserializer(),
                equalTo(innerDataDeserializer));
    }

    @ParameterizedTest
    @EnumSource(value = KafkaBufferMessage.MessageFormat.class,
            names = {"MESSAGE_FORMAT_UNSPECIFIED", "MESSAGE_FORMAT_BYTES"},
            mode = EnumSource.Mode.INCLUDE)
    void deserialize_returns_deserialized_protobuf_data(final KafkaBufferMessage.MessageFormat messageFormat) {
        final byte[] inputProtobufDataBytes = new byte[32];
        random.nextBytes(inputProtobufDataBytes);

        final byte[] serializedInputData = KafkaBufferMessage.BufferData.newBuilder()
                .setData(ByteString.copyFrom(inputProtobufDataBytes))
                .setMessageFormat(messageFormat)
                .build()
                .toByteArray();

        final String expectedDeserializedData = UUID.randomUUID().toString();
        when(innerDataDeserializer.deserialize(topic, inputProtobufDataBytes))
                .thenReturn(expectedDeserializedData);

        assertThat(createObjectUnderTest().deserialize(topic, serializedInputData),
                equalTo(expectedDeserializedData));
    }
}