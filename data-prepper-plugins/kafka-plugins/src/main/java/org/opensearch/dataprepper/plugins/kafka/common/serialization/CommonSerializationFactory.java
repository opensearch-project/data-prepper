package org.opensearch.dataprepper.plugins.kafka.common.serialization;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.opensearch.dataprepper.plugins.kafka.common.KafkaDataConfig;

/**
 * An implementation of {@link SerializationFactory} that can be used in common
 * between all Kafka plugin types.
 */
public class CommonSerializationFactory implements SerializationFactory {
    private final MessageFormatSerializationFactory messageFormatSerializationFactory;
    private final EncryptionSerializationFactory encryptionSerializationFactory;

    public CommonSerializationFactory() {
        this(new MessageFormatSerializationFactory(), new EncryptionSerializationFactory());
    }

    /**
     * Testing constructor.
     *
     * @param messageFormatSerializationFactory The {@link MessageFormatSerializationFactory}
     * @param encryptionSerializationFactory
     */
    CommonSerializationFactory(MessageFormatSerializationFactory messageFormatSerializationFactory, EncryptionSerializationFactory encryptionSerializationFactory) {
        this.messageFormatSerializationFactory = messageFormatSerializationFactory;
        this.encryptionSerializationFactory = encryptionSerializationFactory;
    }

    @Override
    public Deserializer<?> getDeserializer(KafkaDataConfig dataConfig) {
        Deserializer<?> deserializer = messageFormatSerializationFactory.getDeserializer(dataConfig.getSerdeFormat());
        return encryptionSerializationFactory.getDeserializer(dataConfig, deserializer);
    }

    @Override
    public Serializer<?> getSerializer(KafkaDataConfig dataConfig) {
        Serializer<?> serializer = messageFormatSerializationFactory.getSerializer(dataConfig.getSerdeFormat());
        return encryptionSerializationFactory.getSerializer(dataConfig, serializer);
    }
}
