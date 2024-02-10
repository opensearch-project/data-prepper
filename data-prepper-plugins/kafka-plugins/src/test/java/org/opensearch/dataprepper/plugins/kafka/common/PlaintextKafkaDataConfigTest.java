package org.opensearch.dataprepper.plugins.kafka.common;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;

import java.util.UUID;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PlaintextKafkaDataConfigTest {
    @Mock
    private KafkaDataConfig inputDataConfig;

    @ParameterizedTest
    @EnumSource(MessageFormat.class)
    void plaintextDataConfig_returns_KafkaDataConfig_with_getSerdeFormat_returning_PLAINTEXT(MessageFormat inputFormat) {
        final KafkaDataConfig outputDataConfig = PlaintextKafkaDataConfig.plaintextDataConfig(inputDataConfig);

        assertThat(outputDataConfig, notNullValue());

        assertThat(outputDataConfig.getSerdeFormat(), equalTo(MessageFormat.PLAINTEXT));
        verify(inputDataConfig, never()).getSerdeFormat();
    }

    @Test
    void plaintextDataConfig_returns_KafkaDataConfig_with_getEncryptionKeySupplier_returning_from_inner_dataConfig() {
        final Supplier<byte[]> keySupplier = mock(Supplier.class);
        when(inputDataConfig.getEncryptionKeySupplier()).thenReturn(keySupplier);

        final KafkaDataConfig outputDataConfig = PlaintextKafkaDataConfig.plaintextDataConfig(inputDataConfig);

        assertThat(outputDataConfig, notNullValue());
        assertThat(outputDataConfig.getEncryptionKeySupplier(), equalTo(keySupplier));
    }

    @Test
    void plaintextDataConfig_returns_KafkaDataConfig_with_getEncryptionKey_returning_from_inner_dataConfig() {
        final String encryptionKey = UUID.randomUUID().toString();
        when(inputDataConfig.getEncryptedDataKey()).thenReturn(encryptionKey);

        final KafkaDataConfig outputDataConfig = PlaintextKafkaDataConfig.plaintextDataConfig(inputDataConfig);

        assertThat(outputDataConfig, notNullValue());
        assertThat(outputDataConfig.getEncryptedDataKey(), equalTo(encryptionKey));
    }
}