package org.opensearch.dataprepper.plugins.kafka.common.serialization;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.kafka.common.KafkaDataConfig;

import java.security.Key;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EncryptionSerializationFactoryTest {
    @Mock
    private KafkaDataConfig dataConfig;

    private EncryptionSerializationFactory createObjectUnderTest() {
        return new EncryptionSerializationFactory();
    }

    @Test
    void getDeserializer_returns_innerDeserializer_if_encryptionKey_is_null() {
        Deserializer innerDeserializer = mock(Deserializer.class);

        assertThat(createObjectUnderTest().getDeserializer(dataConfig, innerDeserializer),
                equalTo(innerDeserializer));
    }

    @Test
    void getDeserializer_returns_DecryptionDeserializer_if_encryptionKey_is_present() {
        Deserializer innerDeserializer = mock(Deserializer.class);

        byte[] encryptionKey = UUID.randomUUID().toString().getBytes();
        when(dataConfig.getEncryptionKeySupplier()).thenReturn(() -> encryptionKey);
        EncryptionContext encryptionContext = mock(EncryptionContext.class);
        Key key = mock(Key.class);
        when(encryptionContext.getEncryptionKey()).thenReturn(key);

        EncryptionSerializationFactory objectUnderTest = createObjectUnderTest();

        Deserializer<?> actualDeserializer;

        try(MockedStatic<EncryptionContext> encryptionContextMockedStatic = mockStatic(EncryptionContext.class)) {
            encryptionContextMockedStatic.when(() -> EncryptionContext.fromEncryptionKey(encryptionKey))
                    .thenReturn(encryptionContext);
            actualDeserializer = objectUnderTest.getDeserializer(dataConfig, innerDeserializer);
        }

        assertThat(actualDeserializer, instanceOf(DecryptionDeserializer.class));
        DecryptionDeserializer decryptionDeserializer = (DecryptionDeserializer) actualDeserializer;

        assertThat(decryptionDeserializer.getEncryptionContext(), notNullValue());
        assertThat(decryptionDeserializer.getEncryptionContext().getEncryptionKey(), equalTo(key));
        assertThat(decryptionDeserializer.getInnerDeserializer(), equalTo(innerDeserializer));
    }

    @Test
    void getSerializer_returns_innerSerializer_if_encryptionKey_is_null() {
        Serializer innerSerializer = mock(Serializer.class);

        assertThat(createObjectUnderTest().getSerializer(dataConfig, innerSerializer),
                equalTo(innerSerializer));
    }

    @Test
    void getSerializer_returns_EncryptionSerializer_if_encryptionKey_is_present() {
        Serializer innerSerializer = mock(Serializer.class);

        byte[] encryptionKey = UUID.randomUUID().toString().getBytes();
        when(dataConfig.getEncryptionKeySupplier()).thenReturn(() -> encryptionKey);
        EncryptionContext encryptionContext = mock(EncryptionContext.class);
        Key key = mock(Key.class);
        when(encryptionContext.getEncryptionKey()).thenReturn(key);

        EncryptionSerializationFactory objectUnderTest = createObjectUnderTest();

        Serializer<?> actualSerializer;

        try(MockedStatic<EncryptionContext> encryptionContextMockedStatic = mockStatic(EncryptionContext.class)) {
            encryptionContextMockedStatic.when(() -> EncryptionContext.fromEncryptionKey(encryptionKey))
                    .thenReturn(encryptionContext);
            actualSerializer = objectUnderTest.getSerializer(dataConfig, innerSerializer);
        }

        assertThat(actualSerializer, instanceOf(EncryptionSerializer.class));
        EncryptionSerializer encryptionSerializer = (EncryptionSerializer) actualSerializer;

        assertThat(encryptionSerializer.getEncryptionContext(), notNullValue());
        assertThat(encryptionSerializer.getEncryptionContext().getEncryptionKey(), equalTo(key));
        assertThat(encryptionSerializer.getInnerSerializer(), equalTo(innerSerializer));
    }
}