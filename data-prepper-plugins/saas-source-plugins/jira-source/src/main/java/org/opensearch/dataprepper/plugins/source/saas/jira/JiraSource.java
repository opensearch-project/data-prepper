package org.opensearch.dataprepper.plugins.source.saas.jira;


import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.source.saas.crawler.SaasCrawlerApplicationContextMarker;
import org.opensearch.dataprepper.plugins.source.saas.crawler.base.Crawler;
import org.opensearch.dataprepper.plugins.source.saas.crawler.base.SaasPluginExecutorServiceProvider;
import org.opensearch.dataprepper.plugins.source.saas.crawler.base.SaasSourcePlugin;
import org.opensearch.dataprepper.plugins.source.saas.jira.models.JiraOauthConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.opensearch.dataprepper.plugins.source.saas.jira.utils.Constants.OAUTH2;


/**
 * JiraConnector connector entry point.
 */

@DataPrepperPlugin(name = "jira",
        pluginType = Source.class,
        pluginConfigurationType = JiraSourceConfig.class,
        packagesToScan = {SaasCrawlerApplicationContextMarker.class, JiraSource.class}
)
public class JiraSource extends SaasSourcePlugin {

  private static final Logger log = LoggerFactory.getLogger(JiraSource.class);
  private final JiraSourceConfig jiraSourceConfig;
  private final JiraOauthConfig jiraOauthConfig;

  @DataPrepperPluginConstructor
  public JiraSource(final PluginMetrics pluginMetrics,
                    final JiraSourceConfig jiraSourceConfig,
                    final JiraOauthConfig jiraOauthConfig,
                    final PluginFactory pluginFactory,
                    final AcknowledgementSetManager acknowledgementSetManager,
                    Crawler crawler,
                    SaasPluginExecutorServiceProvider executorServiceProvider) {
    super(pluginMetrics, jiraSourceConfig, pluginFactory, acknowledgementSetManager, crawler, executorServiceProvider);
    log.info("Create Jira Source Connector");
    this.jiraSourceConfig = jiraSourceConfig;
    this.jiraOauthConfig = jiraOauthConfig;
  }

  @Override
  public void start(Buffer<Record<Event>> buffer) {
    log.info("Starting Jira Source Plugin... ");
    JiraConfigHelper.validateConfig(jiraSourceConfig);
    if(this.jiraSourceConfig.getAuthType().equals(OAUTH2)) {
      jiraOauthConfig.initAuthBasedUrl();
    }
    super.start(buffer);
  }

  @Override
  public void stop() {
    super.stop();
  }

}
