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
import org.opensearch.dataprepper.plugins.kafka.buffer.KafkaBufferMessage;
import org.opensearch.dataprepper.plugins.kafka.common.KafkaDataConfig;

import java.util.Random;
import java.util.UUID;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BufferMessageSerializerTest {
    @Mock
    private Serializer<Object> innerDataSerializer;

    @Mock
    private KafkaDataConfig dataConfig;

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
        return new BufferMessageSerializer<>(innerDataSerializer, dataConfig);
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

        assertThat(actualBufferedData.getEncrypted(), equalTo(false));
        assertThat(actualBufferedData.getEncryptedDataKey(), equalTo(ByteString.empty()));

        verify(dataConfig, never()).getEncryptedDataKey();
    }

    @Test
    void serialize_returns_bytes_wrapped_in_KafkaBufferMessage_with_encryption_data_when_encrypted() throws InvalidProtocolBufferException {
        final byte[] expectedBytes = new byte[32];
        random.nextBytes(expectedBytes);
        final String encryptionKey = UUID.randomUUID().toString();
        when(innerDataSerializer.serialize(topic, inputData)).thenReturn(expectedBytes);
        when(dataConfig.getEncryptionKeySupplier()).thenReturn(mock(Supplier.class));
        when(dataConfig.getEncryptedDataKey()).thenReturn(encryptionKey);
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
        assertThat(actualBufferedData.getEncryptedDataKey(), equalTo(ByteString.copyFromUtf8(encryptionKey)));
    }

    @Test
    void serialize_returns_bytes_wrapped_in_KafkaBufferMessage_without_encryption_data_when_encrypted_but_data_key_is_plaintext() throws InvalidProtocolBufferException {
        final byte[] expectedBytes = new byte[32];
        random.nextBytes(expectedBytes);
        final String encryptionKey = UUID.randomUUID().toString();
        when(innerDataSerializer.serialize(topic, inputData)).thenReturn(expectedBytes);
        when(dataConfig.getEncryptionKeySupplier()).thenReturn(mock(Supplier.class));
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
        assertThat(actualBufferedData.getEncryptedDataKey(), equalTo(ByteString.empty()));
    }

    @Test
    void serialize_with_null_returns_null_and_does_not_call_inner_dataSerializer() {
        assertThat(createObjectUnderTest().serialize(topic, null),
                nullValue());

        verifyNoInteractions(innerDataSerializer);
    }
}