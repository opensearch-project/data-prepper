package org.opensearch.dataprepper.plugins.kafka.common.key;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;

import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KeyFactoryTest {
    private List<InnerKeyProvider> innerKeyProviders;
    @Mock
    private TopicConfig topicConfig;

    @BeforeEach
    void setUp() {
        innerKeyProviders = List.of(
                mock(InnerKeyProvider.class),
                mock(InnerKeyProvider.class),
                mock(InnerKeyProvider.class)
        );
    }

    private KeyFactory createObjectUnderTest() {
        return new KeyFactory(innerKeyProviders);
    }

    @Test
    void getKeySupplier_returns_null_if_encryptionKey_is_null() {
        assertThat(createObjectUnderTest().getKeySupplier(topicConfig),
                nullValue());
    }

    @Test
    void getKeySupplier_returns_using_first_InnerKeyFactory_that_supports_the_TopicConfig() {
        when(topicConfig.getEncryptionKey()).thenReturn(UUID.randomUUID().toString());

        InnerKeyProvider middleKeyProvider = innerKeyProviders.get(1);
        when(middleKeyProvider.supportsConfiguration(topicConfig)).thenReturn(true);

        Supplier<byte[]> keySupplier = createObjectUnderTest().getKeySupplier(topicConfig);

        assertThat(keySupplier, notNullValue());

        byte[] expectedBytes = UUID.randomUUID().toString().getBytes();
        when(middleKeyProvider.apply(topicConfig)).thenReturn(expectedBytes);
        assertThat(keySupplier.get(), equalTo(expectedBytes));

        InnerKeyProvider lastKeyProvider = innerKeyProviders.get(2);
        verifyNoInteractions(lastKeyProvider);
    }

    @Test
    void getEncryptedDataKey_returns_null_if_encryptionKey_is_null() {
        assertThat(createObjectUnderTest().getEncryptedDataKey(topicConfig),
                nullValue());
    }

    @Test
    void getEncryptedDataKey_returns_null_if_encryptionKey_is_present_and_innerKeyProvider_indicates_unencrypted_data_key() {
        when(topicConfig.getEncryptionKey()).thenReturn(UUID.randomUUID().toString());

        final InnerKeyProvider middleKeyProvider = innerKeyProviders.get(1);
        when(middleKeyProvider.supportsConfiguration(topicConfig)).thenReturn(true);
        when(middleKeyProvider.isKeyEncrypted()).thenReturn(false);

        assertThat(createObjectUnderTest().getEncryptedDataKey(topicConfig),
                nullValue());
    }

    @Test
    void getEncryptedDataKey_returns_null_if_encryptionKey_is_present_and_innerKeyProvider_indicates_encrypted_data_key() {
        final String encryptionKey = UUID.randomUUID().toString();
        when(topicConfig.getEncryptionKey()).thenReturn(encryptionKey);

        final InnerKeyProvider middleKeyProvider = innerKeyProviders.get(1);
        when(middleKeyProvider.supportsConfiguration(topicConfig)).thenReturn(true);
        when(middleKeyProvider.isKeyEncrypted()).thenReturn(true);

        assertThat(createObjectUnderTest().getEncryptedDataKey(topicConfig),
                equalTo(encryptionKey));
    }
}