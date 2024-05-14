package org.opensearch.dataprepper.plugins.mongo.documentdb;

import org.opensearch.dataprepper.common.concurrent.BackgroundThreadFactory;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.mongo.configuration.CollectionConfig;
import org.opensearch.dataprepper.plugins.mongo.configuration.MongoDBSourceConfig;
import org.opensearch.dataprepper.plugins.mongo.leader.LeaderScheduler;
import org.opensearch.dataprepper.plugins.mongo.s3partition.S3PartitionCreatorScheduler;
import org.opensearch.dataprepper.plugins.mongo.utils.DocumentDBSourceAggregateMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class DocumentDBService {
    private static final Logger LOG = LoggerFactory.getLogger(DocumentDBService.class);
    private static final String S3_PATH_DELIMITER = "/";
    private final EnhancedSourceCoordinator sourceCoordinator;
    private final PluginMetrics pluginMetrics;
    private final MongoDBSourceConfig sourceConfig;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final PluginConfigObservable pluginConfigObservable;
    private final DocumentDBSourceAggregateMetrics documentDBAggregateMetrics;
    private ExecutorService leaderExecutor;
    public DocumentDBService(final EnhancedSourceCoordinator sourceCoordinator,
                             final MongoDBSourceConfig sourceConfig,
                             final PluginMetrics pluginMetrics,
                             final AcknowledgementSetManager acknowledgementSetManager,
                             final PluginConfigObservable pluginConfigObservable) {
        this.sourceCoordinator = sourceCoordinator;
        this.pluginMetrics = pluginMetrics;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.sourceConfig = sourceConfig;
        this.pluginConfigObservable = pluginConfigObservable;
        this.documentDBAggregateMetrics = new DocumentDBSourceAggregateMetrics();
    }

    /**
     * This service start three long-running threads (scheduler)
     * Each thread is responsible for one type of job.
     * The data will be guaranteed to be sent to {@link Buffer} in order.
     *
     * @param buffer Data Prepper Buffer
     */
    public void start(Buffer<Record<Event>> buffer) {
        final List<Runnable> runnableList = new ArrayList<>();

        final String s3PathPrefix = getS3PathPrefix();
        final LeaderScheduler leaderScheduler = new LeaderScheduler(sourceCoordinator, sourceConfig, s3PathPrefix);
        runnableList.add(leaderScheduler);
        final List<String> collections = sourceConfig.getCollections().stream().map(CollectionConfig::getCollection).collect(Collectors.toList());
        if (!collections.isEmpty()) {
            final S3PartitionCreatorScheduler s3PartitionCreatorScheduler = new S3PartitionCreatorScheduler(sourceCoordinator, collections);
            runnableList.add(s3PartitionCreatorScheduler);
        }
        leaderExecutor = Executors.newFixedThreadPool(runnableList.size(),
                BackgroundThreadFactory.defaultExecutorThreadFactory("documentdb-source"));
        runnableList.forEach(leaderExecutor::submit);

        final MongoTasksRefresher mongoTasksRefresher = new MongoTasksRefresher(
                buffer, sourceCoordinator, pluginMetrics, acknowledgementSetManager,
                numThread -> Executors.newFixedThreadPool(
                        numThread, BackgroundThreadFactory.defaultExecutorThreadFactory("documentdb-source")),
                s3PathPrefix, documentDBAggregateMetrics);
        mongoTasksRefresher.initialize(sourceConfig);
        pluginConfigObservable.addPluginConfigObserver(
                pluginConfig -> mongoTasksRefresher.update((MongoDBSourceConfig) pluginConfig));
    }

    private String getS3PathPrefix() {
        final String s3UserPathPrefix;
        if (sourceConfig.getS3Prefix() != null && !sourceConfig.getS3Prefix().isBlank()) {
            s3UserPathPrefix = sourceConfig.getS3Prefix() + S3_PATH_DELIMITER;
        } else {
            s3UserPathPrefix = "";
        }

        final String s3PathPrefix;
        final Instant now = Instant.now();
        if (sourceCoordinator.getPartitionPrefix() != null ) {
            s3PathPrefix = s3UserPathPrefix + sourceCoordinator.getPartitionPrefix() + S3_PATH_DELIMITER + now.toEpochMilli() + S3_PATH_DELIMITER;
        } else {
            s3PathPrefix = s3UserPathPrefix + now.toEpochMilli() + S3_PATH_DELIMITER;
        }
        return s3PathPrefix;
    }

    /**
     * Interrupt the running of schedulers.
     * Each scheduler must implement logic for gracefully shutdown.
     */
    public void shutdown() {
        if (leaderExecutor != null) {
            LOG.info("shutdown DocumentDB Service scheduler and worker");
            leaderExecutor.shutdownNow();
        }
    }
}
