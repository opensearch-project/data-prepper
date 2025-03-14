/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.confluence;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.atlassian.configuration.AuthenticationConfig;
import org.opensearch.dataprepper.plugins.source.atlassian.configuration.BasicConfig;
import org.opensearch.dataprepper.plugins.source.atlassian.rest.auth.AtlassianAuthConfig;
import org.opensearch.dataprepper.plugins.source.confluence.models.ConfluenceServerMetadata;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.Crawler;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.PluginExecutorServiceProvider;

import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.atlassian.rest.auth.AtlassianOauthConfig.ACCESSIBLE_RESOURCES;
import static org.opensearch.dataprepper.plugins.source.confluence.utils.Constants.BASIC;

@ExtendWith(MockitoExtension.class)
public class ConfluenceSourceTest {

    @Mock
    Buffer<Record<Event>> buffer;
    @Mock
    AuthenticationConfig authenticationConfig;
    @Mock
    BasicConfig basicConfig;
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private ConfluenceSourceConfig confluenceSourceConfig;
    @Mock
    private AtlassianAuthConfig jiraOauthConfig;
    @Mock
    private PluginFactory pluginFactory;
    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;
    @Mock
    private Crawler crawler;
    @Mock
    private EnhancedSourceCoordinator sourceCooridinator;
    @Mock
    private PluginExecutorServiceProvider executorServiceProvider;
    @Mock
    private ConfluenceService service;
    @Mock
    private ExecutorService executorService;
    @Mock
    private ConfluenceServerMetadata serverMetadata;

    @Test
    void initialization() {
        when(executorServiceProvider.get()).thenReturn(executorService);
        ConfluenceSource source = new ConfluenceSource(pluginMetrics, confluenceSourceConfig, jiraOauthConfig, pluginFactory, acknowledgementSetManager, crawler, executorServiceProvider, service);
        assertNotNull(source);
    }

    @Test
    void testStart() {
        when(executorServiceProvider.get()).thenReturn(executorService);
        when(service.getConfluenceServerMetadata()).thenReturn(serverMetadata);
        ConfluenceSource source = new ConfluenceSource(pluginMetrics, confluenceSourceConfig, jiraOauthConfig, pluginFactory, acknowledgementSetManager, crawler, executorServiceProvider, service);
        when(confluenceSourceConfig.getAccountUrl()).thenReturn(ACCESSIBLE_RESOURCES);
        when(confluenceSourceConfig.getAuthType()).thenReturn(BASIC);
        when(confluenceSourceConfig.getAuthenticationConfig()).thenReturn(authenticationConfig);
        when(authenticationConfig.getBasicConfig()).thenReturn(basicConfig);
        when(basicConfig.getUsername()).thenReturn("Test Id");
        when(basicConfig.getPassword()).thenReturn("Test Credential");

        source.setEnhancedSourceCoordinator(sourceCooridinator);
        source.start(buffer);
        verify(executorService, atLeast(1)).submit(any(Runnable.class));
    }

    @Test
    void testStop() {
        when(executorServiceProvider.get()).thenReturn(executorService);
        when(service.getConfluenceServerMetadata()).thenReturn(serverMetadata);
        ConfluenceSource source = new ConfluenceSource(pluginMetrics, confluenceSourceConfig, jiraOauthConfig, pluginFactory, acknowledgementSetManager, crawler, executorServiceProvider, service);
        when(confluenceSourceConfig.getAccountUrl()).thenReturn(ACCESSIBLE_RESOURCES);
        when(confluenceSourceConfig.getAuthType()).thenReturn(BASIC);
        when(confluenceSourceConfig.getAuthenticationConfig()).thenReturn(authenticationConfig);
        when(authenticationConfig.getBasicConfig()).thenReturn(basicConfig);
        when(basicConfig.getUsername()).thenReturn("Test Id");
        when(basicConfig.getPassword()).thenReturn("Test Credential");

        source.setEnhancedSourceCoordinator(sourceCooridinator);
        source.start(buffer);
        source.stop();
        verify(executorService).shutdownNow();
    }

    @Test
    void testStop_WhenNotStarted() {
        when(executorServiceProvider.get()).thenReturn(executorService);
        ConfluenceSource source = new ConfluenceSource(pluginMetrics, confluenceSourceConfig, jiraOauthConfig, pluginFactory, acknowledgementSetManager, crawler, executorServiceProvider, service);

        source.stop();

        verify(executorService, never()).shutdown();
    }
}