package org.opensearch.dataprepper.plugins.source.saas.jira;


import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.ByteDecoder;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.source.source_crawler.SaasCrawlerApplicationContextMarker;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.Crawler;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.SaasPluginExecutorServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * JiraConnector connector entry point.
 */

@DataPrepperPlugin(name = "jira",
        pluginType = Source.class,
        packagesToScan = {SaasCrawlerApplicationContextMarker.class, JiraSource.class}
)
public class JiraSource implements Source<Record<Event>> {

  private static final Logger log = LoggerFactory.getLogger(JiraSource.class);


  @DataPrepperPluginConstructor
  public JiraSource(final PluginMetrics pluginMetrics,
                    final PluginFactory pluginFactory,
                    final AcknowledgementSetManager acknowledgementSetManager,
                    Crawler crawler,
                    SaasPluginExecutorServiceProvider executorServiceProvider) {
    log.info("Create Jira Source Connector");
  }

  public void start(Buffer<Record<Event>> buffer) {
    log.info("Starting Jira Source Plugin... ");
  }

  @Override
  public void stop() {

  }

  @Override
  public ByteDecoder getDecoder() {
    return Source.super.getDecoder();
  }

}
