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
import org.opensearch.dataprepper.plugins.source.jira.rest.auth.JiraAuthConfig;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.Crawler;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.CrawlerClient;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.PluginExecutorServiceProvider;
import org.slf4j.Logger;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.ACCESSIBLE_RESOURCES;
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

    private PluginExecutorServiceProvider executorServiceProvider = new PluginExecutorServiceProvider();

    @Test
    void initialization() {
        JiraSource source = new JiraSource(pluginMetrics, jiraSourceConfig, jiraOauthConfig, pluginFactory, acknowledgementSetManager, crawler, executorServiceProvider);
        assertNotNull(source);
    }

//    @Test
//    void testStartStopCycle() {
//        JiraSource source = new JiraSource(pluginMetrics, jiraSourceConfig, jiraOauthConfig, pluginFactory, acknowledgementSetManager, crawler, executorServiceProvider);
//        Buffer<Record<Event>> mockBuffer = mock(Buffer.class);
//        when(jiraSourceConfig.getAccountUrl()).thenReturn(ACCESSIBLE_RESOURCES);
//        when(jiraSourceConfig.getAuthType()).thenReturn(BASIC);
//        when(jiraSourceConfig.getJiraId()).thenReturn("Test Jira Id");
//        when(jiraSourceConfig.getJiraCredential()).thenReturn("Test Jira Credentials");
////
//        EnhancedSourceCoordinator mockCoordinator = mock(EnhancedSourceCoordinator.class);
//
//        source.start(mockBuffer);
////
//        source.stop();
//
//        source.start(mockBuffer);
//
//    }
}
