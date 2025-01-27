/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.jira;

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
import org.opensearch.dataprepper.plugins.source.jira.configuration.AuthenticationConfig;
import org.opensearch.dataprepper.plugins.source.jira.configuration.BasicConfig;
import org.opensearch.dataprepper.plugins.source.jira.rest.JiraRestClient;
import org.opensearch.dataprepper.plugins.source.jira.rest.auth.JiraAuthConfig;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.Crawler;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.PluginExecutorServiceProvider;

import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.jira.rest.auth.JiraOauthConfig.ACCESSIBLE_RESOURCES;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.BASIC;

@ExtendWith(MockitoExtension.class)
public class JiraSourceTest {

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private JiraSourceConfig jiraSourceConfig;

    @Mock
    private JiraAuthConfig jiraOauthConfig;

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private Crawler crawler;

    @Mock
    private EnhancedSourceCoordinator sourceCooridinator;

    @Mock
    Buffer<Record<Event>> buffer;

    @Mock
    private PluginExecutorServiceProvider executorServiceProvider;

    @Mock
    private ExecutorService executorService;

    @Mock
    AuthenticationConfig authenticationConfig;

    @Mock
    BasicConfig basicConfig;

    @Mock
    private JiraService jiraService;

    @Mock
    private JiraRestClient jiraRestClient;

    @Test
    void initialization() {
        when(executorServiceProvider.get()).thenReturn(executorService);
        JiraSource source = new JiraSource(pluginMetrics, jiraSourceConfig, jiraOauthConfig, pluginFactory, acknowledgementSetManager, crawler, executorServiceProvider, jiraService, jiraRestClient);
        assertNotNull(source);
    }

    @Test
    void testStart() {
        when(executorServiceProvider.get()).thenReturn(executorService);
        JiraSource source = new JiraSource(pluginMetrics, jiraSourceConfig, jiraOauthConfig, pluginFactory, acknowledgementSetManager, crawler, executorServiceProvider, jiraService, jiraRestClient);
        when(jiraSourceConfig.getAccountUrl()).thenReturn(ACCESSIBLE_RESOURCES);
        when(jiraSourceConfig.getAuthType()).thenReturn(BASIC);
        when(jiraSourceConfig.getAuthenticationConfig()).thenReturn(authenticationConfig);
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
        JiraSource source = new JiraSource(pluginMetrics, jiraSourceConfig, jiraOauthConfig, pluginFactory, acknowledgementSetManager, crawler, executorServiceProvider, jiraService, jiraRestClient);
        when(jiraSourceConfig.getAccountUrl()).thenReturn(ACCESSIBLE_RESOURCES);
        when(jiraSourceConfig.getAuthType()).thenReturn(BASIC);
        when(jiraSourceConfig.getAuthenticationConfig()).thenReturn(authenticationConfig);
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
        JiraSource source = new JiraSource(pluginMetrics, jiraSourceConfig, jiraOauthConfig, pluginFactory, acknowledgementSetManager, crawler, executorServiceProvider, jiraService, jiraRestClient);

        source.stop();

        verify(executorService, never()).shutdown();
    }
}