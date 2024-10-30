package org.opensearch.dataprepper.plugins.source.jira;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.plugins.source.jira.rest.auth.JiraAuthConfig;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.Crawler;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.PluginExecutorServiceProvider;

import static org.junit.jupiter.api.Assertions.assertNotNull;

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
}
