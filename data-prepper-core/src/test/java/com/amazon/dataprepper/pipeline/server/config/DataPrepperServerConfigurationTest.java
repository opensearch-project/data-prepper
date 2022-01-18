/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.pipeline.server.config;

import com.amazon.dataprepper.DataPrepper;
import com.amazon.dataprepper.model.configuration.PluginModel;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.plugin.PluginFactory;
import com.amazon.dataprepper.pipeline.server.DataPrepperCoreAuthenticationProvider;
import com.amazon.dataprepper.pipeline.server.HttpServerProvider;
import com.amazon.dataprepper.pipeline.server.ListPipelinesHandler;
import com.amazon.dataprepper.pipeline.server.PrometheusMetricsHandler;
import com.amazon.dataprepper.pipeline.server.ShutdownHandler;
import com.sun.net.httpserver.Authenticator;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DataPrepperServerConfigurationTest {
    @Mock
    private HttpContext context;

    @Mock
    private HttpServer httpServer;

    @Mock
    private HttpServerProvider httpServerProvider;

    @Mock
    private ListPipelinesHandler listPipelinesHandler;

    private final DataPrepperServerConfiguration serverConfiguration = new DataPrepperServerConfiguration();

    @Test
    public void testGivenNullPrometheusMeterRegistryAndNullAuthenticatorThenServerIsCreated() {
        when(httpServerProvider.get())
                .thenReturn(httpServer);
        final HttpServer server = serverConfiguration.httpServer(httpServerProvider, listPipelinesHandler, null, null);

        assertThat(server, is(httpServer));
        verify(server).createContext("/list", listPipelinesHandler);

    }

    @Test
    public void testGivenPrometheusMeterRegistryAndNullAuthenticatorThenServerIsCreated() {
        final PrometheusMeterRegistry meterRegistry = mock(PrometheusMeterRegistry.class);

        when(httpServerProvider.get())
                .thenReturn(httpServer);
        when(httpServer.createContext(any(String.class), any(HttpHandler.class)))
                .thenReturn(context);

        final HttpServer server = serverConfiguration.httpServer(httpServerProvider, listPipelinesHandler, meterRegistry, null);

        assertThat(server, is(httpServer));
        verify(server).createContext(eq("/list"), eq(listPipelinesHandler));
        verify(server).createContext(eq("/metrics/prometheus"), any(PrometheusMetricsHandler.class));
        verify(server).createContext(eq("/metrics/sys"), any(PrometheusMetricsHandler.class));
        verifyNoInteractions(context);
    }

    @Test
    public void testGivenPrometheusMeterRegistryAndAuthenticatorThenServerIsCreated() {
        final PrometheusMeterRegistry meterRegistry = mock(PrometheusMeterRegistry.class);
        final Authenticator authenticator = mock(Authenticator.class);

        when(httpServerProvider.get())
                .thenReturn(httpServer);
        when(httpServer.createContext(any(String.class), any(HttpHandler.class)))
                .thenReturn(context);

        final HttpServer server = serverConfiguration.httpServer(httpServerProvider, listPipelinesHandler, meterRegistry, authenticator);

        assertThat(server, is(httpServer));
        verify(server).createContext(eq("/list"), eq(listPipelinesHandler));
        verify(server).createContext(eq("/metrics/prometheus"), any(PrometheusMetricsHandler.class));
        verify(server).createContext(eq("/metrics/sys"), any(PrometheusMetricsHandler.class));
        verify(context, times(3)).setAuthenticator(eq(authenticator));
    }

    @Test
    public void testGivingNoConfigThenCreateInsecureSettings() {
        final PluginSetting pluginSetting = serverConfiguration.pluginSetting(Optional.empty());

        assertThat(pluginSetting.getName(), is("unauthenticated"));
        assertThat(pluginSetting.getSettings().isEmpty(), is(true));
    }

    @Test
    public void testGivingInsecureConfigThenCreateInsecureSettings() {
        final PluginModel pluginModel = mock(PluginModel.class);

        when(pluginModel.getPluginName())
                .thenReturn("unauthenticated");

        final PluginSetting pluginSetting = serverConfiguration.pluginSetting(Optional.of(pluginModel));

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

        final PluginSetting pluginSetting = serverConfiguration.pluginSetting(Optional.of(pluginModel));

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
        final DataPrepper dataPrepper = mock(DataPrepper.class);

        final ListPipelinesHandler handler = serverConfiguration.listPipelinesHandler(dataPrepper);

        assertThat(handler, isA(ListPipelinesHandler.class));
    }

    @Test
    public void testGivenValidInputWithAuthenticatorThenServerListContextCreated() {
        final DataPrepper dataPrepper = mock(DataPrepper.class);

        final ListPipelinesHandler handler = serverConfiguration.listPipelinesHandler(dataPrepper);

        assertThat(handler, isA(ListPipelinesHandler.class));
    }

    @Test
    public void testGivenValidInputWithNoAuthenticatorThenServerShutdownContextCreated() {
        final DataPrepper dataPrepper = mock(DataPrepper.class);
        final HttpServer server = mock(HttpServer.class);
        final HttpContext context = mock(HttpContext.class);

        when(server.createContext(eq("/shutdown"), any(ShutdownHandler.class)))
                .thenReturn(context);

        final ShutdownHandler handler = serverConfiguration.shutdownHandler(dataPrepper, Optional.empty(), server);

        assertThat(handler, isA(ShutdownHandler.class));
        verifyNoInteractions(context);
    }

    @Test
    public void testGivenValidInputWithAuthenticatorThenServerShutdownContextCreated() {
        final DataPrepper dataPrepper = mock(DataPrepper.class);
        final Authenticator authenticator = mock(Authenticator.class);
        final HttpServer server = mock(HttpServer.class);
        final HttpContext context = mock(HttpContext.class);

        when(server.createContext(eq("/shutdown"), any(ShutdownHandler.class)))
                .thenReturn(context);

        final ShutdownHandler handler = serverConfiguration.shutdownHandler(dataPrepper, Optional.of(authenticator), server);

        assertThat(handler, isA(ShutdownHandler.class));
        verify(context)
                .setAuthenticator(eq(authenticator));
    }
}