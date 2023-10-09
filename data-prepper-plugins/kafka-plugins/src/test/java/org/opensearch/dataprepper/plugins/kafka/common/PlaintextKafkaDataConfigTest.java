package org.opensearch.dataprepper.plugins.kafka.common;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PlaintextKafkaDataConfigTest {
    @ParameterizedTest
    @EnumSource(MessageFormat.class)
    void plaintextDataConfig_returns_KafkaDataConfig_with_getSerdeFormat_returning_PLAINTEXT(MessageFormat inputFormat) {
        KafkaDataConfig inputDataConfig = mock(KafkaDataConfig.class);
        when(inputDataConfig.getSerdeFormat()).thenReturn(inputFormat);

        KafkaDataConfig outputDataConfig = PlaintextKafkaDataConfig.plaintextDataConfig(inputDataConfig);

        assertThat(outputDataConfig, notNullValue());

        assertThat(outputDataConfig.getSerdeFormat(), equalTo(MessageFormat.PLAINTEXT));
    }
}