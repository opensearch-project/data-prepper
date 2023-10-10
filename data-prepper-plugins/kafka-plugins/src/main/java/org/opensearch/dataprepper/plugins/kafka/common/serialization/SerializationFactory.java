package org.opensearch.dataprepper.plugins.kafka.common.serialization;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.opensearch.dataprepper.plugins.kafka.common.KafkaDataConfig;

public class SerializationFactory {
    private final MessageFormatSerializationFactory messageFormatSerializationFactory;
    private final EncryptionSerializationFactory encryptionSerializationFactory;

    public SerializationFactory() {
        this(new MessageFormatSerializationFactory(), new EncryptionSerializationFactory());
    }

    /**
     * Testing constructor.
     *
     * @param messageFormatSerializationFactory The {@link MessageFormatSerializationFactory}
     * @param encryptionSerializationFactory
     */
    SerializationFactory(MessageFormatSerializationFactory messageFormatSerializationFactory, EncryptionSerializationFactory encryptionSerializationFactory) {
        this.messageFormatSerializationFactory = messageFormatSerializationFactory;
        this.encryptionSerializationFactory = encryptionSerializationFactory;
    }

    public Deserializer<?> getDeserializer(KafkaDataConfig dataConfig) {
        Deserializer<?> deserializer = messageFormatSerializationFactory.getDeserializer(dataConfig.getSerdeFormat());
        return encryptionSerializationFactory.getDeserializer(dataConfig, deserializer);
    }

    public Serializer<?> getSerializer(KafkaDataConfig dataConfig) {
        Serializer<?> serializer = messageFormatSerializationFactory.getSerializer(dataConfig.getSerdeFormat());
        return encryptionSerializationFactory.getSerializer(dataConfig, serializer);
    }
}
