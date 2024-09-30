package org.opensearch.dataprepper.plugins.source.saas.crawler.base;


import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.annotations.PluginDiContextAware;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.ByteDecoder;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.UsesEnhancedSourceCoordination;
import org.opensearch.dataprepper.plugins.source.saas.crawler.coordination.LeaderPartition;
import org.opensearch.dataprepper.plugins.source.saas.crawler.coordination.LeaderProgressState;
import org.opensearch.dataprepper.plugins.source.saas.crawler.coordination.LeaderScheduler;
import org.opensearch.dataprepper.plugins.source.saas.crawler.coordination.PartitionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;



/**
 * JiraConnector connector entry point.
 */

public class BaseSaasSourcePlugin implements Source<Record<Event>>, UsesEnhancedSourceCoordination, PluginDiContextAware {


  private static final Logger log = LoggerFactory.getLogger(BaseSaasSourcePlugin.class);
  private static final int DEFAULT_THREAD_COUNT = 20;
  private final PluginMetrics pluginMetrics;
  private final PluginFactory pluginFactory;
//  private final JiraService jiraService;

  private AnnotationConfigApplicationContext pluginDIContext;

  private final AcknowledgementSetManager acknowledgementSetManager;

  private final ExecutorService executorService;

  private EnhancedSourceCoordinator coordinator;

  private BaseSaasSourceConfig sourceConfig;

  private Buffer<Record<Event>> buffer;


  @DataPrepperPluginConstructor
  public BaseSaasSourcePlugin(final PluginMetrics pluginMetrics,
                              final BaseSaasSourceConfig sourceConfig,
                              final PluginFactory pluginFactory,
                              final AcknowledgementSetManager acknowledgementSetManager) {
    log.info("Create Jira Source Connector");
    this.pluginMetrics = pluginMetrics;
    this.sourceConfig = sourceConfig;
    this.pluginFactory = pluginFactory;

    this.acknowledgementSetManager = acknowledgementSetManager;
    this.executorService = Executors.newFixedThreadPool(DEFAULT_THREAD_COUNT);

  }

  @Override
  public void start(Buffer<Record<Event>> buffer) {
    Objects.requireNonNull(coordinator);
    log.info("Starting Jira Service... ");
    this.buffer = buffer;

    boolean isPartitionCreated = coordinator.createPartition(new LeaderPartition());
    log.info("Leader partition creation status: {}", isPartitionCreated);

    Crawler crawler = pluginDIContext.getBean(Crawler.class);
    log.info("Crawler bean instance {}", crawler);

    Runnable leaderScheduler = new LeaderScheduler(coordinator, this);
    this.executorService.submit(leaderScheduler);
  }


  public void init(LeaderPartition leaderPartition) {
    Objects.requireNonNull(buffer);

    //ConnectorConfiguration connectorConfiguration = sourceConfig.getRepositoryConfiguration();

    //this.executorService.submit(this.createMonitoringLoop(connectorId, connectorConfiguration, buffer));

    log.debug("Update initialization state");
    LeaderProgressState leaderProgressState = leaderPartition.getProgressState().get();
    leaderProgressState.setInitialized(true);
  }



  @Override
  public void stop() {
    log.info("Stop Source Connector");
  }

  @Override
  public boolean areAcknowledgementsEnabled() {
    return Source.super.areAcknowledgementsEnabled();
  }




  @Override
  public void setEnhancedSourceCoordinator(EnhancedSourceCoordinator sourceCoordinator) {
    coordinator = sourceCoordinator;
    coordinator.initialize();
  }

  @Override
  public Function<SourcePartitionStoreItem, EnhancedSourcePartition> getPartitionFactory() {
    return new PartitionFactory();
  }

  @Override
  public ByteDecoder getDecoder() {
    return Source.super.getDecoder();
  }


  @Override
  public void setPluginDIContext(AnnotationConfigApplicationContext pluginDIContext) {
    this.pluginDIContext = pluginDIContext;
  }
}
