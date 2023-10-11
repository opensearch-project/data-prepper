package org.opensearch.dataprepper.plugins.kafka.common.serialization;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.opensearch.dataprepper.plugins.kafka.common.KafkaDataConfig;

import javax.crypto.NoSuchPaddingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

class EncryptionSerializationFactory {
    Deserializer<?> getDeserializer(KafkaDataConfig dataConfig, Deserializer<?> innerDeserializer) {
        if(dataConfig.getEncryptionKeySupplier() == null)
            return innerDeserializer;

        EncryptionContext encryptionContext = EncryptionContext.fromEncryptionKey(dataConfig.getEncryptionKeySupplier().get());

        try {
            return new DecryptionDeserializer<>(innerDeserializer, encryptionContext);
        } catch (InvalidKeyException | NoSuchPaddingException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    Serializer<?> getSerializer(KafkaDataConfig dataConfig, Serializer<?> innerSerializer) {
        if(dataConfig.getEncryptionKeySupplier() == null)
            return innerSerializer;

        EncryptionContext encryptionContext = EncryptionContext.fromEncryptionKey(dataConfig.getEncryptionKeySupplier().get());

        try {
            return new EncryptionSerializer<>(innerSerializer, encryptionContext);
        } catch (InvalidKeyException | NoSuchPaddingException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
