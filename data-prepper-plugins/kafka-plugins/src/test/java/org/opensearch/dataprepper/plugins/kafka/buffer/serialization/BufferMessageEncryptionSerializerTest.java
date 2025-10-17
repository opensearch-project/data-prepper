/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.buffer.serialization;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.encryption.EncryptionEngine;
import org.opensearch.dataprepper.model.encryption.EncryptionEnvelope;
import org.opensearch.dataprepper.plugins.encryption.DefaultEncryptionEnvelope;
import org.opensearch.dataprepper.plugins.kafka.buffer.KafkaBufferMessage;
import org.opensearch.dataprepper.plugins.kafka.common.KafkaDataConfig;

import java.util.Random;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BufferMessageEncryptionSerializerTest {
    @Mock
    private Serializer<Object> innerDataSerializer;

    @Mock
    private KafkaDataConfig dataConfig;

    @Mock
    private EncryptionEngine encryptionEngine;

    @Mock
    private EncryptionEnvelope encryptionEnvelope;

    private String topic;
    private Object inputData;
    private Random random;

    @BeforeEach
    void setUp() {
        random = new Random();

        topic = UUID.randomUUID().toString();
        inputData = UUID.randomUUID().toString();
    }

    private BufferMessageEncryptionSerializer<Object> createObjectUnderTest() {
        return new BufferMessageEncryptionSerializer<>(innerDataSerializer, encryptionEngine);
    }

    @Test
    void constructor_throws_if_innerDataSerializer_is_null() {
        innerDataSerializer = null;
        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void constructor_throws_if_dataConfig_is_null() {
        innerDataSerializer = null;
        assertThrows(NullPointerException.class, this::createObjectUnderTest);
    }

    @Test
    void getDataSerializer_returns_innerDataSerializer_to_help_with_other_tests() {
        assertThat(createObjectUnderTest().getDataSerializer(),
                equalTo(innerDataSerializer));
    }

    @Test
    void serialize_returns_bytes_wrapped_in_KafkaBufferMessage() throws InvalidProtocolBufferException {
        final String encryptedDataKey = UUID.randomUUID().toString();
        final byte[] expectedBytes = new byte[32];
        random.nextBytes(expectedBytes);
        when(innerDataSerializer.serialize(topic, inputData)).thenReturn(expectedBytes);
        when(encryptionEngine.encrypt(eq(expectedBytes))).thenReturn(
                DefaultEncryptionEnvelope.builder()
                        .encryptedData(expectedBytes)
                        .encryptedDataKey(encryptedDataKey)
                        .build());
        final byte[] actualBytes = createObjectUnderTest().serialize(topic, inputData);

        assertThat(actualBytes, notNullValue());

        final KafkaBufferMessage.BufferData actualBufferedData =
                KafkaBufferMessage.BufferData.parseFrom(actualBytes);

        assertThat(actualBufferedData.getMessageFormat(),
                equalTo(KafkaBufferMessage.MessageFormat.MESSAGE_FORMAT_BYTES));

        assertThat(actualBufferedData.getData(), notNullValue());
        assertThat(actualBufferedData.getData().toByteArray(),
                equalTo(expectedBytes));

        assertThat(actualBufferedData.getEncrypted(), equalTo(true));
        assertThat(actualBufferedData.getEncryptedDataKey(), equalTo(ByteString.copyFromUtf8(encryptedDataKey)));

        verify(dataConfig, never()).getEncryptedDataKey();
    }

    @Test
    void serialize_with_null_returns_null_and_does_not_call_inner_dataSerializer() {
        assertThat(createObjectUnderTest().serialize(topic, null),
                nullValue());

        verifyNoInteractions(innerDataSerializer);
    }
}