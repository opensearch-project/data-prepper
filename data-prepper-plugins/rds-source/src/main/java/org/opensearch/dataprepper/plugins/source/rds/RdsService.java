/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.rds.export.DataFileScheduler;
import org.opensearch.dataprepper.plugins.source.rds.export.ExportScheduler;
import org.opensearch.dataprepper.plugins.source.rds.leader.LeaderScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RdsService {
    private static final Logger LOG = LoggerFactory.getLogger(RdsService.class);

    private final RdsClient rdsClient;
    private final S3Client s3Client;
    private final EnhancedSourceCoordinator sourceCoordinator;
    private final EventFactory eventFactory;
    private final PluginMetrics pluginMetrics;
    private final RdsSourceConfig sourceConfig;
    private ExecutorService executor;
    private LeaderScheduler leaderScheduler;
    private ExportScheduler exportScheduler;
    private DataFileScheduler dataFileScheduler;

    public RdsService(final EnhancedSourceCoordinator sourceCoordinator,
                      final RdsSourceConfig sourceConfig,
                      final EventFactory eventFactory,
                      final ClientFactory clientFactory,
                      final PluginMetrics pluginMetrics) {
        this.sourceCoordinator = sourceCoordinator;
        this.eventFactory = eventFactory;
        this.pluginMetrics = pluginMetrics;
        this.sourceConfig = sourceConfig;

        rdsClient = clientFactory.buildRdsClient();
        s3Client = clientFactory.buildS3Client();
    }

    /**
     * This service start three long-running threads (scheduler)
     * Each thread is responsible for one type of job.
     * The data will be guaranteed to be sent to {@link Buffer} in order.
     *
     * @param buffer Data Prepper Buffer
     */
    public void start(Buffer<Record<Event>> buffer) {
        LOG.info("Start running RDS service");
        final List<Runnable> runnableList = new ArrayList<>();
        leaderScheduler = new LeaderScheduler(sourceCoordinator, sourceConfig);
        exportScheduler = new ExportScheduler(sourceCoordinator, rdsClient, s3Client, pluginMetrics);
        dataFileScheduler = new DataFileScheduler(sourceCoordinator, sourceConfig, s3Client, eventFactory, buffer);
        runnableList.add(leaderScheduler);
        runnableList.add(exportScheduler);
        runnableList.add(dataFileScheduler);

        executor = Executors.newFixedThreadPool(runnableList.size());
        runnableList.forEach(executor::submit);
    }

    /**
     * Interrupt the running of schedulers.
     * Each scheduler must implement logic for gracefully shutdown.
     */
    public void shutdown() {
        if (executor != null) {
            LOG.info("shutdown RDS schedulers");
            exportScheduler.shutdown();
            leaderScheduler.shutdown();
            dataFileScheduler.shutdown();
            executor.shutdownNow();
        }
    }
}
