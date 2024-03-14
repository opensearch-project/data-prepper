package org.opensearch.dataprepper.plugins.mongo.documentdb;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.mongo.export.MongoDBExportPartitionSupplier;
import org.opensearch.dataprepper.plugins.mongo.configuration.MongoDBSourceConfig;
import org.opensearch.dataprepper.plugins.mongo.export.ExportScheduler;
import org.opensearch.dataprepper.plugins.mongo.export.ExportWorker;
import org.opensearch.dataprepper.plugins.mongo.leader.LeaderScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DocumentDBService {
    private static final Logger LOG = LoggerFactory.getLogger(DocumentDBService.class);
    private final EnhancedSourceCoordinator sourceCoordinator;
    private final PluginMetrics pluginMetrics;
    private final MongoDBSourceConfig sourceConfig;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final ExecutorService executor;
    private ExportScheduler exportScheduler;
    private ExportWorker exportWorker;
    private LeaderScheduler leaderScheduler;
    private final MongoDBExportPartitionSupplier mongoDBExportPartitionSupplier;
    public DocumentDBService(final EnhancedSourceCoordinator sourceCoordinator,
                             final MongoDBSourceConfig sourceConfig,
                             final PluginMetrics pluginMetrics,
                             final AcknowledgementSetManager acknowledgementSetManager) {
        this.sourceCoordinator = sourceCoordinator;
        this.pluginMetrics = pluginMetrics;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.sourceConfig = sourceConfig;

        this.mongoDBExportPartitionSupplier = new MongoDBExportPartitionSupplier(sourceConfig);
        executor = Executors.newFixedThreadPool(3);
    }

    /**
     * This service start three long-running threads (scheduler)
     * Each thread is responsible for one type of job.
     * The data will be guaranteed to be sent to {@link Buffer} in order.
     *
     * @param buffer Data Prepper Buffer
     */
    public void start(Buffer<Record<Event>> buffer) {
        this.exportScheduler = new ExportScheduler(sourceCoordinator, mongoDBExportPartitionSupplier, pluginMetrics);
        this.exportWorker = new ExportWorker(sourceCoordinator, buffer, pluginMetrics, acknowledgementSetManager, sourceConfig);
        this.leaderScheduler = new LeaderScheduler(sourceCoordinator, sourceConfig.getCollections());

        executor.submit(leaderScheduler);
        executor.submit(exportScheduler);
        executor.submit(exportWorker);
    }

    /**
     * Interrupt the running of schedulers.
     * Each scheduler must implement logic for gracefully shutdown.
     */
    public void shutdown() {
        LOG.info("shutdown DocumentDB Service scheduler and worker");
        executor.shutdownNow();
    }
}
