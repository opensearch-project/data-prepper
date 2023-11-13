/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.buffer.serialization;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.kafka.buffer.KafkaBufferMessage;

import java.util.Random;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BufferMessageSerializerTest {
    @Mock
    private Serializer<Object> innerDataSerializer;

    private String topic;
    private Object inputData;
    private Random random;

    @BeforeEach
    void setUp() {
        random = new Random();

        topic = UUID.randomUUID().toString();
        inputData = UUID.randomUUID().toString();
    }

    private BufferMessageSerializer<Object> createObjectUnderTest() {
        return new BufferMessageSerializer<>(innerDataSerializer);
    }

    @Test
    void getDataSerializer_returns_innerDataSerializer_to_help_with_other_tests() {
        assertThat(createObjectUnderTest().getDataSerializer(),
                equalTo(innerDataSerializer));
    }

    @Test
    void serialize_returns_bytes_wrapped_in_KafkaBufferMessage() throws InvalidProtocolBufferException {
        final byte[] expectedBytes = new byte[32];
        random.nextBytes(expectedBytes);
        when(innerDataSerializer.serialize(topic, inputData)).thenReturn(expectedBytes);
        final byte[] actualBytes = createObjectUnderTest().serialize(topic, inputData);

        assertThat(actualBytes, notNullValue());

        final KafkaBufferMessage.BufferData actualBufferedData =
                KafkaBufferMessage.BufferData.parseFrom(actualBytes);

        assertThat(actualBufferedData.getMessageFormat(),
                equalTo(KafkaBufferMessage.MessageFormat.MESSAGE_FORMAT_BYTES));

        assertThat(actualBufferedData.getData(), notNullValue());
        assertThat(actualBufferedData.getData().toByteArray(),
                equalTo(expectedBytes));
    }
}