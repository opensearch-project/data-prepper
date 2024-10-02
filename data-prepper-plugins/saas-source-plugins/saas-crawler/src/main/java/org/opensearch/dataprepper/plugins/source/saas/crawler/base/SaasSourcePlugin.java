package org.opensearch.dataprepper.plugins.source.saas.crawler.base;


import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
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
import org.opensearch.dataprepper.plugins.source.saas.crawler.coordination.LeaderScheduler;
import org.opensearch.dataprepper.plugins.source.saas.crawler.coordination.PartitionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;



/**
 * JiraConnector connector entry point.
 */

public class SaasSourcePlugin implements Source<Record<Event>>, UsesEnhancedSourceCoordination {


  private static final Logger log = LoggerFactory.getLogger(SaasSourcePlugin.class);
  private static final int DEFAULT_THREAD_COUNT = 20;
  private final PluginMetrics pluginMetrics;
  private final PluginFactory pluginFactory;

  private final AcknowledgementSetManager acknowledgementSetManager;

  private final ExecutorService executorService;

  private EnhancedSourceCoordinator coordinator;

  private final SaasSourceConfig sourceConfig;

  private Buffer<Record<Event>> buffer;
  private final Crawler crawler;


  @DataPrepperPluginConstructor
  public SaasSourcePlugin(final PluginMetrics pluginMetrics,
                          final SaasSourceConfig sourceConfig,
                          final PluginFactory pluginFactory,
                          final AcknowledgementSetManager acknowledgementSetManager,
                          Crawler crawler) {
    log.info("Create Jira Source Connector");
    this.pluginMetrics = pluginMetrics;
    this.sourceConfig = sourceConfig;
    this.pluginFactory = pluginFactory;
    this.crawler = crawler;

    this.acknowledgementSetManager = acknowledgementSetManager;
    this.executorService = Executors.newFixedThreadPool(DEFAULT_THREAD_COUNT);
  }

  @Override
  public void start(Buffer<Record<Event>> buffer) {
    Objects.requireNonNull(coordinator);
    log.info("Starting SaaS Source Plugin... ");
    this.buffer = buffer;

    boolean isPartitionCreated = coordinator.createPartition(new LeaderPartition());
    log.info("Leader partition creation status: {}", isPartitionCreated);

    Runnable leaderScheduler = new LeaderScheduler(coordinator, this, crawler);
    this.executorService.submit(leaderScheduler);
    //Register worker threaders
    for(int i=0; i< sourceConfig.DEFAULT_NUMBER_OF_WORKERS; i++) {
      SourceItemWorker sourceItemWorker = new SourceItemWorker(buffer, coordinator, sourceConfig);
      this.executorService.submit(new Thread(sourceItemWorker));
    }
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

  public SaasSourceConfig getSourceConfig() {
    return sourceConfig;
  }

}
