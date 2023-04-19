/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.server.config;

import org.opensearch.dataprepper.DataPrepper;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.pipeline.PipelinesProvider;
import org.opensearch.dataprepper.pipeline.server.DataPrepperCoreAuthenticationProvider;
import org.opensearch.dataprepper.pipeline.server.ListPipelinesHandler;
import org.opensearch.dataprepper.pipeline.server.ShutdownHandler;
import com.sun.net.httpserver.Authenticator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DataPrepperServerConfigurationTest {
    private final DataPrepperServerConfiguration serverConfiguration = new DataPrepperServerConfiguration();

    @Test
    public void testGivingNoConfigThenCreateInsecureSettings() {
        final PluginSetting pluginSetting = serverConfiguration.pluginSetting(null);

        assertThat(pluginSetting.getName(), is("unauthenticated"));
        assertThat(pluginSetting.getSettings().isEmpty(), is(true));
    }

    @Test
    public void testGivingInsecureConfigThenCreateInsecureSettings() {
        final PluginModel pluginModel = mock(PluginModel.class);

        when(pluginModel.getPluginName())
                .thenReturn("unauthenticated");

        final PluginSetting pluginSetting = serverConfiguration.pluginSetting(pluginModel);

        assertThat(pluginSetting.getName(), is("unauthenticated"));
        assertThat(pluginSetting.getSettings().isEmpty(), is(true));
    }

    @Test
    public void testGivingSecureConfigThenCreateInsecureSettings() {
        final PluginModel pluginModel = mock(PluginModel.class);
        final Map<String, Object> settings = new HashMap<>();

        when(pluginModel.getPluginName())
                .thenReturn("super secure plugin");

        when(pluginModel.getPluginSettings())
                .thenReturn(settings);

        final PluginSetting pluginSetting = serverConfiguration.pluginSetting(pluginModel);

        assertThat(pluginSetting.getName(), is("super secure plugin"));
        assertThat(pluginSetting.getSettings(), is(settings));
    }

    @Test
    public void testGivenPluginFactoryAndPluginSettingsThenCreateAuthenticationProvider() {
        final PluginFactory pluginFactory = mock(PluginFactory.class);
        final PluginSetting pluginSetting = mock(PluginSetting.class);
        final DataPrepperCoreAuthenticationProvider expected = mock(DataPrepperCoreAuthenticationProvider.class);

        when(pluginFactory.loadPlugin(eq(DataPrepperCoreAuthenticationProvider.class), eq(pluginSetting)))
                .thenReturn(expected);

        final DataPrepperCoreAuthenticationProvider authenticationProvider = serverConfiguration.authenticationProvider(
                pluginFactory,
                pluginSetting);

        assertThat(authenticationProvider, is(expected));
    }

    @Test
    public void testGivenGetAuthenticatorReturnsValueThenReturnOptionalContainingValue() {
        final DataPrepperCoreAuthenticationProvider provider = mock(DataPrepperCoreAuthenticationProvider.class);
        final Authenticator authenticatorMock = mock(Authenticator.class);

        when(provider.getAuthenticator())
                .thenReturn(authenticatorMock);

        final Authenticator authenticator = serverConfiguration.authenticator(provider);

        assertThat(authenticator, is(authenticatorMock));
    }

    @Test
    public void testGivenValidInputWithNoAuthenticatorThenServerListContextCreated() {
        final PipelinesProvider pipelinesProvider = mock(PipelinesProvider.class);

        final ListPipelinesHandler handler = serverConfiguration.listPipelinesHandler(pipelinesProvider);

        assertThat(handler, isA(ListPipelinesHandler.class));
    }

    @Test
    public void testGivenValidInputWithAuthenticatorThenServerListContextCreated() {
        final PipelinesProvider pipelinesProvider = mock(PipelinesProvider.class);

        final ListPipelinesHandler handler = serverConfiguration.listPipelinesHandler(pipelinesProvider);

        assertThat(handler, isA(ListPipelinesHandler.class));
    }

    @Test
    public void testShutdownHandlerIsCreated() {
        final DataPrepper dataPrepper = mock(DataPrepper.class);

        final ShutdownHandler handler = serverConfiguration.shutdownHandler(dataPrepper);

        assertThat(handler, isA(ShutdownHandler.class));
    }
}