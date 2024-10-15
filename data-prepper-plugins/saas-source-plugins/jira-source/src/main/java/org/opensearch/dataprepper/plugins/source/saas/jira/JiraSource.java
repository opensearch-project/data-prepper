package org.opensearch.dataprepper.plugins.source.saas.jira;


import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.source.saas.crawler.SaasCrawlerApplicationContextMarker;
import org.opensearch.dataprepper.plugins.source.saas.crawler.base.Crawler;
import org.opensearch.dataprepper.plugins.source.saas.crawler.base.SaasPluginExecutorServiceProvider;
import org.opensearch.dataprepper.plugins.source.saas.crawler.base.SaasSourcePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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

  @DataPrepperPluginConstructor
  public JiraSource(final PluginMetrics pluginMetrics,
                    final JiraSourceConfig jiraSourceConfig,
                    final PluginFactory pluginFactory,
                    final AcknowledgementSetManager acknowledgementSetManager,
                    Crawler crawler,
                    SaasPluginExecutorServiceProvider executorServiceProvider,
                    JiraService service) {
    super(pluginMetrics, jiraSourceConfig, pluginFactory, acknowledgementSetManager, crawler, executorServiceProvider);
    log.info("Create Jira Source Connector");

    //Handshake with the service
    service.handShakeWithService(jiraSourceConfig);
  }

}
