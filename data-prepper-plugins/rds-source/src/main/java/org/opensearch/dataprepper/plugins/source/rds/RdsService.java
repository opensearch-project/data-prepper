/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.rds.export.ClusterSnapshotStrategy;
import org.opensearch.dataprepper.plugins.source.rds.export.DataFileScheduler;
import org.opensearch.dataprepper.plugins.source.rds.export.ExportScheduler;
import org.opensearch.dataprepper.plugins.source.rds.export.ExportTaskManager;
import org.opensearch.dataprepper.plugins.source.rds.export.InstanceSnapshotStrategy;
import org.opensearch.dataprepper.plugins.source.rds.export.SnapshotManager;
import org.opensearch.dataprepper.plugins.source.rds.export.SnapshotStrategy;
import org.opensearch.dataprepper.plugins.source.rds.leader.LeaderScheduler;
import org.opensearch.dataprepper.plugins.source.rds.stream.BinlogClientFactory;
import org.opensearch.dataprepper.plugins.source.rds.stream.StreamScheduler;
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

    /**
     * Maximum concurrent data loader per node
     */
    public static final int DATA_LOADER_MAX_JOB_COUNT = 1;

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
    private StreamScheduler streamScheduler;

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
        runnableList.add(leaderScheduler);

        if (sourceConfig.isExportEnabled()) {
            final SnapshotStrategy snapshotStrategy = sourceConfig.isCluster() ?
                    new ClusterSnapshotStrategy(rdsClient) : new InstanceSnapshotStrategy(rdsClient);
            final SnapshotManager snapshotManager = new SnapshotManager(snapshotStrategy);
            final ExportTaskManager exportTaskManager = new ExportTaskManager(rdsClient);
            exportScheduler = new ExportScheduler(
                    sourceCoordinator, snapshotManager, exportTaskManager, s3Client, pluginMetrics);
            dataFileScheduler = new DataFileScheduler(
                    sourceCoordinator, sourceConfig, s3Client, eventFactory, buffer);
            runnableList.add(exportScheduler);
            runnableList.add(dataFileScheduler);
        }

        if (sourceConfig.isStreamEnabled()) {
            BinaryLogClient binaryLogClient = new BinlogClientFactory(sourceConfig, rdsClient).create();
            if (!sourceConfig.getTlsConfig().isInsecure()) {
                binaryLogClient.setSSLMode(SSLMode.REQUIRED);
            }
            streamScheduler = new StreamScheduler(sourceCoordinator, sourceConfig, binaryLogClient, buffer, pluginMetrics);
            runnableList.add(streamScheduler);
        }

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
            if (sourceConfig.isExportEnabled()) {
                exportScheduler.shutdown();
                dataFileScheduler.shutdown();
            }

            if (sourceConfig.isStreamEnabled()) {
                streamScheduler.shutdown();
            }

            leaderScheduler.shutdown();
            executor.shutdownNow();
        }
    }
}
