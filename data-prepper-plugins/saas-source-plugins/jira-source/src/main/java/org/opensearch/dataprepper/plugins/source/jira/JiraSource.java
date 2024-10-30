package org.opensearch.dataprepper.plugins.source.jira;


import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.source.jira.rest.auth.JiraAuthConfig;
import org.opensearch.dataprepper.plugins.source.jira.utils.JiraConfigHelper;
import org.opensearch.dataprepper.plugins.source.source_crawler.CrawlerApplicationContextMarker;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.Crawler;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.CrawlerSourcePlugin;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.PluginExecutorServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.opensearch.dataprepper.plugins.source.jira.utils.Constants.PLUGIN_NAME;


/**
 * JiraConnector connector entry point.
 */

@DataPrepperPlugin(name = PLUGIN_NAME,
        pluginType = Source.class,
        pluginConfigurationType = JiraSourceConfig.class,
        packagesToScan = {CrawlerApplicationContextMarker.class, JiraSource.class}
)
public class JiraSource extends CrawlerSourcePlugin {

    private static final Logger log = LoggerFactory.getLogger(JiraSource.class);
    private final JiraSourceConfig jiraSourceConfig;
    private final JiraAuthConfig jiraOauthConfig;

    @DataPrepperPluginConstructor
    public JiraSource(final PluginMetrics pluginMetrics,
                      final JiraSourceConfig jiraSourceConfig,
                      final JiraAuthConfig jiraOauthConfig,
                      final PluginFactory pluginFactory,
                      final AcknowledgementSetManager acknowledgementSetManager,
                      Crawler crawler,
                      PluginExecutorServiceProvider executorServiceProvider) {
        super(PLUGIN_NAME, pluginMetrics, jiraSourceConfig, pluginFactory, acknowledgementSetManager, crawler, executorServiceProvider);
        log.info("Creating Jira Source Plugin");
        this.jiraSourceConfig = jiraSourceConfig;
        this.jiraOauthConfig = jiraOauthConfig;
    }

    @Override
    public void start(Buffer<Record<Event>> buffer) {
        log.info("Starting Jira Source Plugin... ");
        JiraConfigHelper.validateConfig(jiraSourceConfig);
        jiraOauthConfig.initCredentials();
        super.start(buffer);
    }

    @Override
    public void stop() {
        super.stop();
    }

}
