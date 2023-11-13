package org.opensearch.dataprepper.plugins.kafka.common.serialization;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.kafka.common.KafkaDataConfig;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CommonSerializationFactoryTest {
    @Mock
    private MessageFormatSerializationFactory messageFormatSerializationFactory;
    @Mock
    private EncryptionSerializationFactory encryptionSerializationFactory;

    @Mock
    private KafkaDataConfig kafkaDataConfig;

    private SerializationFactory createObjectUnderTest() {
        return new CommonSerializationFactory(messageFormatSerializationFactory, encryptionSerializationFactory);
    }

    @ParameterizedTest
    @EnumSource(MessageFormat.class)
    void getDeserializer_returns_result_of_EncryptionSerializationFactory_from_MessageFormatSerializationFactory(MessageFormat serdeFormat) {
        Deserializer innerDeserializer = mock(Deserializer.class);
        Deserializer deserializer = mock(Deserializer.class);
        when(kafkaDataConfig.getSerdeFormat()).thenReturn(serdeFormat);

        when(messageFormatSerializationFactory.getDeserializer(serdeFormat))
                .thenReturn(innerDeserializer);
        when(encryptionSerializationFactory.getDeserializer(kafkaDataConfig, innerDeserializer))
                .thenReturn(deserializer);

        assertThat(createObjectUnderTest().getDeserializer(kafkaDataConfig),
                equalTo(deserializer));
    }

    @ParameterizedTest
    @EnumSource(MessageFormat.class)
    void getSerializer_returns_result_of_EncryptionSerializationFactory_from_MessageFormatSerializationFactory(MessageFormat serdeFormat) {
        Serializer innerSerializer = mock(Serializer.class);
        Serializer serializer = mock(Serializer.class);
        when(kafkaDataConfig.getSerdeFormat()).thenReturn(serdeFormat);

        when(messageFormatSerializationFactory.getSerializer(serdeFormat))
                .thenReturn(innerSerializer);
        when(encryptionSerializationFactory.getSerializer(kafkaDataConfig, innerSerializer))
                .thenReturn(serializer);

        assertThat(createObjectUnderTest().getSerializer(kafkaDataConfig),
                equalTo(serializer));
    }
}