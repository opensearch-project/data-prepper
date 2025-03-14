package org.opensearch.dataprepper.plugins.source.source_crawler.base;


import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
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
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.PartitionFactory;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.scheduler.LeaderScheduler;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.scheduler.WorkerScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;


/**
 * JiraConnector connector entry point.
 */

public abstract class CrawlerSourcePlugin implements Source<Record<Event>>, UsesEnhancedSourceCoordination {


    private static final Logger log = LoggerFactory.getLogger(CrawlerSourcePlugin.class);
    private EnhancedSourceCoordinator coordinator;
    private Optional<SourceServerMetadata> serverMetadata = Optional.empty();
    private final PluginMetrics pluginMetrics;
    private final PluginFactory pluginFactory;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final ExecutorService executorService;
    private final CrawlerSourceConfig sourceConfig;
    private final Crawler crawler;
    private final String sourcePluginName;
    private final int batchSize;


    public CrawlerSourcePlugin(final String sourcePluginName,
                               final PluginMetrics pluginMetrics,
                               final CrawlerSourceConfig sourceConfig,
                               final PluginFactory pluginFactory,
                               final AcknowledgementSetManager acknowledgementSetManager,
                               final Crawler crawler,
                               final PluginExecutorServiceProvider executorServiceProvider) {
        log.debug("Creating {} Source Plugin", sourcePluginName);
        this.sourcePluginName = sourcePluginName;
        this.pluginMetrics = pluginMetrics;
        this.sourceConfig = sourceConfig;
        this.pluginFactory = pluginFactory;
        this.crawler = crawler;
        this.batchSize = sourceConfig.getBatchSize();

        this.acknowledgementSetManager = acknowledgementSetManager;
        this.executorService = executorServiceProvider.get();
    }

    public void setServerMetadata(SourceServerMetadata serverMetadata) {
        this.serverMetadata = Optional.of(serverMetadata);
    }


    @Override
    public void start(Buffer<Record<Event>> buffer) {
        Objects.requireNonNull(coordinator);
        log.info("Starting {} Source Plugin", sourcePluginName);
        Duration timezoneOffset = Duration.ofSeconds(0);

        boolean isPartitionCreated = coordinator.createPartition(new LeaderPartition());
        log.debug("Leader partition creation status: {}", isPartitionCreated);
        if (serverMetadata.isPresent()) {
            timezoneOffset = serverMetadata.get().getPollingTimezoneOffset();
        }

        Runnable leaderScheduler = new LeaderScheduler(coordinator, crawler, batchSize, timezoneOffset);
        this.executorService.submit(leaderScheduler);
        //Register worker threaders
        for (int i = 0; i < sourceConfig.DEFAULT_NUMBER_OF_WORKERS; i++) {
            WorkerScheduler workerScheduler = new WorkerScheduler(sourcePluginName, buffer, coordinator,
                    sourceConfig, crawler, pluginMetrics, acknowledgementSetManager);
            this.executorService.submit(new Thread(workerScheduler));
        }
    }


    @Override
    public void stop() {
        log.info("Stop Source Connector");
        this.executorService.shutdownNow();
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

}
