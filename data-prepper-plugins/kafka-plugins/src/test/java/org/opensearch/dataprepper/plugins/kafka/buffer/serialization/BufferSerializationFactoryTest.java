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
import org.opensearch.dataprepper.model.encryption.EncryptionEngine;
import org.opensearch.dataprepper.plugins.encryption.EncryptionSupplier;
import org.opensearch.dataprepper.plugins.kafka.common.KafkaDataConfig;
import org.opensearch.dataprepper.plugins.kafka.common.serialization.SerializationFactory;

import java.util.Random;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BufferSerializationFactoryTest {
    private static final String TEST_ENCRYPTION_ID = "test-encryption-id";
    @Mock
    private SerializationFactory innerSerializationFactory;
    @Mock
    private EncryptionSupplier encryptionSupplier;
    @Mock
    private EncryptionEngine encryptionEngine;
    @Mock
    private KafkaDataConfig dataConfig;
    private Random random;

    @BeforeEach
    void setUp() {
        random = new Random();
    }

    private BufferSerializationFactory createObjectUnderTest() {
        return new BufferSerializationFactory(innerSerializationFactory, encryptionSupplier);
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
    void getDeserializer_returns_BufferedMessageEncryptionDeserializer() {
        when(dataConfig.getEncryptionId()).thenReturn(TEST_ENCRYPTION_ID);
        when(encryptionSupplier.getEncryptionEngine(eq(TEST_ENCRYPTION_ID))).thenReturn(encryptionEngine);
        final Deserializer innerDeserializer = mock(Deserializer.class);
        when(innerSerializationFactory.getDeserializer(dataConfig))
                .thenReturn(innerDeserializer);

        final Deserializer<?> actualDeserializer = createObjectUnderTest().getDeserializer(dataConfig);

        assertThat(actualDeserializer, instanceOf(BufferMessageEncryptionDeserializer.class));

        final BufferMessageEncryptionDeserializer bufferMessageEncryptionDeserializer =
                (BufferMessageEncryptionDeserializer) actualDeserializer;

        assertThat(bufferMessageEncryptionDeserializer.getDataDeserializer(), equalTo(innerDeserializer));
    }

    @Test
    void getDeserializer_throws_IllegalArgumentException_due_to_missing_encryptionEngine() {
        when(dataConfig.getEncryptionId()).thenReturn(TEST_ENCRYPTION_ID);
        when(encryptionSupplier.getEncryptionEngine(eq(TEST_ENCRYPTION_ID))).thenReturn(null);
        final Deserializer innerDeserializer = mock(Deserializer.class);
        when(innerSerializationFactory.getDeserializer(dataConfig))
                .thenReturn(innerDeserializer);

        assertThrows(IllegalArgumentException.class, () -> createObjectUnderTest().getDeserializer(dataConfig));
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

    @Test
    void getSerializer_returns_BufferedMessageEncryptionSerializer() {
        when(dataConfig.getEncryptionId()).thenReturn(TEST_ENCRYPTION_ID);
        when(encryptionSupplier.getEncryptionEngine(eq(TEST_ENCRYPTION_ID))).thenReturn(encryptionEngine);
        final Serializer innerSerializer = mock(Serializer.class);
        when(innerSerializationFactory.getSerializer(dataConfig))
                .thenReturn(innerSerializer);

        final Serializer<?> actualSerializer = createObjectUnderTest().getSerializer(dataConfig);

        assertThat(actualSerializer, instanceOf(BufferMessageEncryptionSerializer.class));

        final BufferMessageEncryptionSerializer bufferMessageEncryptionSerializer =
                (BufferMessageEncryptionSerializer) actualSerializer;

        assertThat(bufferMessageEncryptionSerializer.getDataSerializer(), equalTo(innerSerializer));
    }

    @Test
    void getSerializer_throws_IllegalArgumentException_due_to_missing_encryptionEngine() {
        when(dataConfig.getEncryptionId()).thenReturn(TEST_ENCRYPTION_ID);
        when(encryptionSupplier.getEncryptionEngine(eq(TEST_ENCRYPTION_ID))).thenReturn(null);
        final Serializer innerSerializer = mock(Serializer.class);
        when(innerSerializationFactory.getSerializer(dataConfig))
                .thenReturn(innerSerializer);

        assertThrows(IllegalArgumentException.class, () -> createObjectUnderTest().getSerializer(dataConfig));
    }
}