package org.opensearch.dataprepper.plugins.kafka.common;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.kafka.common.key.KeyFactory;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;

import java.util.UUID;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaDataConfigAdapterTest {
    @Mock
    private KeyFactory keyFactory;
    @Mock
    private TopicConfig topicConfig;

    private KafkaDataConfigAdapter createObjectUnderTest() {
        return new KafkaDataConfigAdapter(keyFactory, topicConfig);
    }

    @ParameterizedTest
    @EnumSource(MessageFormat.class)
    void getSerdeFormat_returns_TopicConfig_getSerdeFormat(MessageFormat serdeFormat) {
        when(topicConfig.getSerdeFormat()).thenReturn(serdeFormat);
        assertThat(createObjectUnderTest().getSerdeFormat(),
                equalTo(serdeFormat));
    }

    @Test
    void getEncryptionKeySupplier_returns_null_if_encryptionKey_is_null() {
        assertThat(createObjectUnderTest().getEncryptionKeySupplier(),
                nullValue());
    }

    @Test
    void getEncryptionKeySupplier_returns_keyFactory_getKeySupplier_if_encryptionKey_is_present() {
        String encryptionKey = UUID.randomUUID().toString();
        when(topicConfig.getEncryptionKey()).thenReturn(encryptionKey);
        Supplier<byte[]> keySupplier = mock(Supplier.class);
        when(keyFactory.getKeySupplier(topicConfig)).thenReturn(keySupplier);

        assertThat(createObjectUnderTest().getEncryptionKeySupplier(),
                equalTo(keySupplier));
    }

    @Test
    void getEncryptionKey_returns_null_when_topic_encryptedDataKey_is_null() {
        assertThat(createObjectUnderTest().getEncryptedDataKey(),
                nullValue());
    }

    @Test
    void getEncryptionKey_returns_value_of_getEncryptedDataKey_when_encryptedDataKey_is_not_null() {
        final String encryptionKey = UUID.randomUUID().toString();
        when(topicConfig.getEncryptionKey()).thenReturn(UUID.randomUUID().toString());
        when(keyFactory.getEncryptedDataKey(topicConfig)).thenReturn(encryptionKey);

        assertThat(createObjectUnderTest().getEncryptedDataKey(),
                equalTo(encryptionKey));
    }
}