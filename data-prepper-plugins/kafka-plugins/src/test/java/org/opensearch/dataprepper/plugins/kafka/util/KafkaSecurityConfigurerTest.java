package org.opensearch.dataprepper.plugins.kafka.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.model.plugin.PluginConfigObserver;
import org.opensearch.dataprepper.plugins.kafka.authenticator.DynamicSaslClientCallbackHandler;
import org.opensearch.dataprepper.plugins.kafka.authenticator.DynamicBasicCredentialsProvider;
import org.opensearch.dataprepper.plugins.kafka.configuration.AuthConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaConnectionConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.PlainTextAuthConfig;

import java.util.Properties;

import static org.apache.kafka.common.config.SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

// TODO: unit tests covering all public methods
@ExtendWith(MockitoExtension.class)
class KafkaSecurityConfigurerTest {
    @Mock
    private PluginConfigObservable pluginConfigObservable;
    @Mock
    private DynamicBasicCredentialsProvider dynamicBasicCredentialsProvider;
    @Mock
    private KafkaConnectionConfig kafkaConnectionConfig;
    @Mock
    private AuthConfig authConfig;
    @Mock
    private AuthConfig.SaslAuthConfig saslAuthConfig;
    @Mock
    private PlainTextAuthConfig plainTextAuthConfig;
    @Captor
    private ArgumentCaptor<PluginConfigObserver> pluginConfigObserverArgumentCaptor;

    @Test
    void testSetDynamicSaslClientCallbackHandlerWithNonNullPlainTextAuthConfig() {
        when(kafkaConnectionConfig.getAuthConfig()).thenReturn(authConfig);
        when(authConfig.getSaslAuthConfig()).thenReturn(saslAuthConfig);
        when(saslAuthConfig.getPlainTextAuthConfig()).thenReturn(plainTextAuthConfig);
        final Properties properties = new Properties();
        try (final MockedStatic<DynamicBasicCredentialsProvider> dynamicBasicCredentialsProviderMockedStatic =
                     mockStatic(DynamicBasicCredentialsProvider.class)) {
            dynamicBasicCredentialsProviderMockedStatic.when(DynamicBasicCredentialsProvider::getInstance)
                    .thenReturn(dynamicBasicCredentialsProvider);
            KafkaSecurityConfigurer.setDynamicSaslClientCallbackHandler(
                    properties, kafkaConnectionConfig, pluginConfigObservable);
        }
        assertThat(properties.get(SASL_CLIENT_CALLBACK_HANDLER_CLASS), equalTo(DynamicSaslClientCallbackHandler.class));
        verify(dynamicBasicCredentialsProvider).refresh(kafkaConnectionConfig);
        verify(pluginConfigObservable).addPluginConfigObserver(pluginConfigObserverArgumentCaptor.capture());
        final PluginConfigObserver pluginConfigObserver = pluginConfigObserverArgumentCaptor.getValue();
        final KafkaConnectionConfig newConfig = mock(KafkaConnectionConfig.class);
        pluginConfigObserver.update(newConfig);
        verify(dynamicBasicCredentialsProvider).refresh(newConfig);
    }

    @Test
    void testSetDynamicSaslClientCallbackHandlerWithNullPlainTextAuthConfig() {
        when(kafkaConnectionConfig.getAuthConfig()).thenReturn(authConfig);
        when(authConfig.getSaslAuthConfig()).thenReturn(saslAuthConfig);
        final Properties properties = new Properties();
        KafkaSecurityConfigurer.setDynamicSaslClientCallbackHandler(
                properties, kafkaConnectionConfig, pluginConfigObservable);
        assertThat(properties.isEmpty(), is(true));
        verifyNoInteractions(pluginConfigObservable);
    }

    @Test
    void testSetDynamicSaslClientCallbackHandlerWithNullSaslAuthConfig() {
        when(kafkaConnectionConfig.getAuthConfig()).thenReturn(authConfig);
        final Properties properties = new Properties();
        KafkaSecurityConfigurer.setDynamicSaslClientCallbackHandler(
                properties, kafkaConnectionConfig, pluginConfigObservable);
        assertThat(properties.isEmpty(), is(true));
        verifyNoInteractions(pluginConfigObservable);
    }

    @Test
    void testSetDynamicSaslClientCallbackHandlerWithNullAuthConfig() {
        final Properties properties = new Properties();
        KafkaSecurityConfigurer.setDynamicSaslClientCallbackHandler(
                properties, kafkaConnectionConfig, pluginConfigObservable);
        assertThat(properties.isEmpty(), is(true));
        verifyNoInteractions(pluginConfigObservable);
    }
}