package org.opensearch.dataprepper.plugins.source.saas.jira;


import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.source.saas.crawler.base.BaseConnectorSourcePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * JiraConnector connector entry point.
 */

@DataPrepperPlugin(name = "jira", pluginType = Source.class, pluginConfigurationType = JiraSourceConfig.class)
public class JiraSource extends BaseConnectorSourcePlugin {

  private static final Logger log = LoggerFactory.getLogger(JiraSource.class);

  @DataPrepperPluginConstructor
  public JiraSource(final PluginMetrics pluginMetrics,
                    final JiraSourceConfig jiraSourceConfig,
                    final PluginFactory pluginFactory,
                    final AcknowledgementSetManager acknowledgementSetManager/*,
                    final AwsCredentialsSupplier awsCredentialsSupplier,
                    */) {
    super(pluginMetrics, jiraSourceConfig, pluginFactory, acknowledgementSetManager);
    log.info("Create Jira Source Connector");
  }

}
