package org.opensearch.dataprepper.plugins.kafka.common.serialization;

import org.apache.kafka.common.serialization.Deserializer;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

/**
 * Implementation of Kafka's {@link Deserializer} which decrypts the message
 * before deserializing it.
 *
 * @param <T> - Type to be deserialized into
 * @see EncryptionSerializer
 */
class DecryptionDeserializer<T> implements Deserializer<T> {
    private final Deserializer<T> innerDeserializer;
    private final Cipher cipher;
    private final EncryptionContext encryptionContext;

    DecryptionDeserializer(Deserializer<T> innerDeserializer, EncryptionContext encryptionContext) throws InvalidKeyException, NoSuchPaddingException, NoSuchAlgorithmException {
        this.innerDeserializer = innerDeserializer;
        cipher = encryptionContext.createDecryptionCipher();
        this.encryptionContext = encryptionContext;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        byte[] unencryptedBytes;
        try {
            unencryptedBytes = cipher.doFinal(data);
        } catch (IllegalBlockSizeException | BadPaddingException e) {
            throw new RuntimeException(e);
        }
        return innerDeserializer.deserialize(topic, unencryptedBytes);
    }

    EncryptionContext getEncryptionContext() {
        return encryptionContext;
    }

    Deserializer<T> getInnerDeserializer() {
        return innerDeserializer;
    }
}
