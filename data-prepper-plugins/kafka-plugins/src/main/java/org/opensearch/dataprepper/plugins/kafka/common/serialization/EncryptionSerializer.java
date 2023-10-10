package org.opensearch.dataprepper.plugins.kafka.common.serialization;

import org.apache.kafka.common.serialization.Serializer;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

/**
 * Implementation of Kafka's {@link Serializer} which encrypts data after serializing it.
 *
 * @param <T> - Type to be serialized from
 * @see DecryptionDeserializer
 */
class EncryptionSerializer<T> implements Serializer<T> {
    private final Serializer<T> innerSerializer;
    private final Cipher cipher;
    private final EncryptionContext encryptionContext;

    EncryptionSerializer(Serializer<T> innerSerializer, EncryptionContext encryptionContext) throws InvalidKeyException, NoSuchPaddingException, NoSuchAlgorithmException {
        this.innerSerializer = innerSerializer;
        cipher = encryptionContext.createEncryptionCipher();
        this.encryptionContext = encryptionContext;
    }

    @Override
    public byte[] serialize(String topic, T data) {
        byte[] unencryptedBytes = innerSerializer.serialize(topic, data);
        try {
            return cipher.doFinal(unencryptedBytes);
        } catch (IllegalBlockSizeException | BadPaddingException e) {
            throw new RuntimeException(e);
        }
    }

    public EncryptionContext getEncryptionContext() {
        return encryptionContext;
    }

    public Serializer<T> getInnerSerializer() {
        return innerSerializer;
    }
}
