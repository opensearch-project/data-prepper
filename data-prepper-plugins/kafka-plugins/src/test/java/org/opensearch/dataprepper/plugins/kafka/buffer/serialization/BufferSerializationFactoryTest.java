/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.buffer.serialization;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.kafka.common.KafkaDataConfig;
import org.opensearch.dataprepper.plugins.kafka.common.serialization.SerializationFactory;

import java.util.Random;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BufferSerializationFactoryTest {
    @Mock
    private SerializationFactory innerSerializationFactory;
    @Mock
    private KafkaDataConfig dataConfig;
    private Random random;

    @BeforeEach
    void setUp() {
        random = new Random();
    }

    private BufferSerializationFactory createObjectUnderTest() {
        return new BufferSerializationFactory(innerSerializationFactory);
    }

    @Test
    void getDeserializer_returns_BufferedDataDeserializer() {
        final Deserializer innerDeserializer = mock(Deserializer.class);
        when(innerSerializationFactory.getDeserializer(dataConfig))
                .thenReturn(innerDeserializer);

        final Deserializer<?> actualDeserializer = createObjectUnderTest().getDeserializer(dataConfig);

        assertThat(actualDeserializer, instanceOf(BufferMessageDeserializer.class));

        final BufferMessageDeserializer bufferMessageDeserializer = (BufferMessageDeserializer) actualDeserializer;

        assertThat(bufferMessageDeserializer.getDataDeserializer(), equalTo(innerDeserializer));
    }

    @Test
    void getSerializer_returns_BufferedDataSerializer() {
        final Serializer innerSerializer = mock(Serializer.class);
        when(innerSerializationFactory.getSerializer(dataConfig))
                .thenReturn(innerSerializer);

        final Serializer<?> actualSerializer = createObjectUnderTest().getSerializer(dataConfig);

        assertThat(actualSerializer, instanceOf(BufferMessageSerializer.class));

        final BufferMessageSerializer bufferMessageSerializer = (BufferMessageSerializer) actualSerializer;

        assertThat(bufferMessageSerializer.getDataSerializer(), equalTo(innerSerializer));
    }
}