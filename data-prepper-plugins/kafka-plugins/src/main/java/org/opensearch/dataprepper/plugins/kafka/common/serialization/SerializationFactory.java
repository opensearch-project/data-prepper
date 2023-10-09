package org.opensearch.dataprepper.plugins.kafka.common.serialization;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.opensearch.dataprepper.plugins.kafka.common.KafkaDataConfig;

public class SerializationFactory {
    private final MessageFormatSerializationFactory messageFormatSerializationFactory;

    public SerializationFactory() {
        this(new MessageFormatSerializationFactory());
    }

    /**
     * Testing constructor.
     *
     * @param messageFormatSerializationFactory The {@link MessageFormatSerializationFactory}
     */
    SerializationFactory(MessageFormatSerializationFactory messageFormatSerializationFactory) {
        this.messageFormatSerializationFactory = messageFormatSerializationFactory;
    }

    public Deserializer<?> getDeserializer(KafkaDataConfig dataConfig) {
        return messageFormatSerializationFactory.getDeserializer(dataConfig.getSerdeFormat());
    }

    public Serializer<?> getSerializer(KafkaDataConfig dataConfig) {
        return messageFormatSerializationFactory.getSerializer(dataConfig.getSerdeFormat());
    }
}
