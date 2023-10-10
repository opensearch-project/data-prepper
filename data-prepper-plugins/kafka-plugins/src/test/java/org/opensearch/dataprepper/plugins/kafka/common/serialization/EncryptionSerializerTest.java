package org.opensearch.dataprepper.plugins.kafka.common.serialization;

import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EncryptionSerializerTest {
    @Mock
    private Serializer<Object> innerSerializer;
    @Mock
    private EncryptionContext encryptionContext;
    @Mock
    private Cipher cipher;
    private String topicName;

    @BeforeEach
    void setUp() throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException {
        topicName = UUID.randomUUID().toString();

        when(encryptionContext.createEncryptionCipher()).thenReturn(cipher);
    }

    private EncryptionSerializer<Object> createObjectUnderTest() throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException {
        return new EncryptionSerializer<>(innerSerializer, encryptionContext);
    }

    @Test
    void serialize_performs_cipher_encryption_on_serialized_data() throws IllegalBlockSizeException, BadPaddingException, NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException {
        byte[] plaintextData = UUID.randomUUID().toString().getBytes();
        Object input = UUID.randomUUID().toString();

        when(innerSerializer.serialize(topicName, input))
                .thenReturn(plaintextData);

        byte[] encryptedData = UUID.randomUUID().toString().getBytes();
        when(cipher.doFinal(plaintextData)).thenReturn(encryptedData);

        assertThat(createObjectUnderTest().serialize(topicName, input),
                equalTo(encryptedData));
    }
}