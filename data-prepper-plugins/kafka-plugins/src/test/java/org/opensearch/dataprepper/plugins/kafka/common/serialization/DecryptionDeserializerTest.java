package org.opensearch.dataprepper.plugins.kafka.common.serialization;

import org.apache.kafka.common.serialization.Deserializer;
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
class DecryptionDeserializerTest {
    @Mock
    private Deserializer<Object> innerDeserializer;
    @Mock
    private EncryptionContext encryptionContext;
    @Mock
    private Cipher cipher;
    private String topicName;

    @BeforeEach
    void setUp() throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException {
        topicName = UUID.randomUUID().toString();

        when(encryptionContext.createDecryptionCipher()).thenReturn(cipher);
    }

    private DecryptionDeserializer<Object> createObjectUnderTest() throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException {
        return new DecryptionDeserializer<>(innerDeserializer, encryptionContext);
    }

    @Test
    void deserialize_calls_innerDeserializer_on_cipher_doFinal() throws IllegalBlockSizeException, BadPaddingException, NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException {
        byte[] decryptedData = UUID.randomUUID().toString().getBytes();
        Object deserializedContent = UUID.randomUUID().toString();

        when(innerDeserializer.deserialize(topicName, decryptedData))
                .thenReturn(deserializedContent);

        byte[] inputData = UUID.randomUUID().toString().getBytes();
        when(cipher.doFinal(inputData)).thenReturn(decryptedData);

        assertThat(createObjectUnderTest().deserialize(topicName, inputData),
                equalTo(deserializedContent));
    }
}