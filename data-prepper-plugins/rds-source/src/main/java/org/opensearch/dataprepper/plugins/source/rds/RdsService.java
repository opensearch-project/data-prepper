/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.network.SSLMode;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.rds.export.DataFileScheduler;
import org.opensearch.dataprepper.plugins.source.rds.export.ExportScheduler;
import org.opensearch.dataprepper.plugins.source.rds.export.ExportTaskManager;
import org.opensearch.dataprepper.plugins.source.rds.export.SnapshotManager;
import org.opensearch.dataprepper.plugins.source.rds.leader.ClusterApiStrategy;
import org.opensearch.dataprepper.plugins.source.rds.leader.InstanceApiStrategy;
import org.opensearch.dataprepper.plugins.source.rds.leader.LeaderScheduler;
import org.opensearch.dataprepper.plugins.source.rds.leader.RdsApiStrategy;
import org.opensearch.dataprepper.plugins.source.rds.model.DbMetadata;
import org.opensearch.dataprepper.plugins.source.rds.schema.ConnectionManager;
import org.opensearch.dataprepper.plugins.source.rds.schema.SchemaManager;
import org.opensearch.dataprepper.plugins.source.rds.stream.BinlogClientFactory;
import org.opensearch.dataprepper.plugins.source.rds.stream.StreamScheduler;
import org.opensearch.dataprepper.plugins.source.rds.utils.IdentifierShortener;
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
    public static final String S3_PATH_DELIMITER = "/";
    public static final int MAX_SOURCE_IDENTIFIER_LENGTH = 15;

    private final RdsClient rdsClient;
    private final S3Client s3Client;
    private final EnhancedSourceCoordinator sourceCoordinator;
    private final EventFactory eventFactory;
    private final PluginMetrics pluginMetrics;
    private final RdsSourceConfig sourceConfig;
    private final AcknowledgementSetManager acknowledgementSetManager;
    private ExecutorService executor;
    private LeaderScheduler leaderScheduler;
    private ExportScheduler exportScheduler;
    private DataFileScheduler dataFileScheduler;
    private StreamScheduler streamScheduler;

    public RdsService(final EnhancedSourceCoordinator sourceCoordinator,
                      final RdsSourceConfig sourceConfig,
                      final EventFactory eventFactory,
                      final ClientFactory clientFactory,
                      final PluginMetrics pluginMetrics,
                      final AcknowledgementSetManager acknowledgementSetManager) {
        this.sourceCoordinator = sourceCoordinator;
        this.eventFactory = eventFactory;
        this.pluginMetrics = pluginMetrics;
        this.sourceConfig = sourceConfig;
        this.acknowledgementSetManager = acknowledgementSetManager;

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

        final RdsApiStrategy rdsApiStrategy = sourceConfig.isCluster() ?
                new ClusterApiStrategy(rdsClient) : new InstanceApiStrategy(rdsClient);
        final DbMetadata dbMetadata = rdsApiStrategy.describeDb(sourceConfig.getDbIdentifier());
        final String s3PathPrefix = getS3PathPrefix();
        leaderScheduler = new LeaderScheduler(
                sourceCoordinator, sourceConfig, s3PathPrefix, getSchemaManager(sourceConfig, dbMetadata), dbMetadata);
        runnableList.add(leaderScheduler);

        if (sourceConfig.isExportEnabled()) {
            final SnapshotManager snapshotManager = new SnapshotManager(rdsApiStrategy);
            final ExportTaskManager exportTaskManager = new ExportTaskManager(rdsClient);
            exportScheduler = new ExportScheduler(
                    sourceCoordinator, snapshotManager, exportTaskManager, s3Client, pluginMetrics);
            dataFileScheduler = new DataFileScheduler(
                    sourceCoordinator, sourceConfig, s3PathPrefix, s3Client, eventFactory, buffer, pluginMetrics, acknowledgementSetManager);
            runnableList.add(exportScheduler);
            runnableList.add(dataFileScheduler);
        }

        if (sourceConfig.isStreamEnabled()) {
            BinaryLogClient binaryLogClient = new BinlogClientFactory(sourceConfig, rdsClient, dbMetadata).create();
            if (sourceConfig.isTlsEnabled()) {
                binaryLogClient.setSSLMode(SSLMode.REQUIRED);
            } else {
                binaryLogClient.setSSLMode(SSLMode.DISABLED);
            }
            streamScheduler = new StreamScheduler(
                    sourceCoordinator, sourceConfig, s3PathPrefix, binaryLogClient, buffer, pluginMetrics, acknowledgementSetManager);
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

    private SchemaManager getSchemaManager(final RdsSourceConfig sourceConfig, final DbMetadata dbMetadata) {
        final ConnectionManager connectionManager = new ConnectionManager(
                dbMetadata.getHostName(),
                dbMetadata.getPort(),
                sourceConfig.getAuthenticationConfig().getUsername(),
                sourceConfig.getAuthenticationConfig().getPassword(),
                sourceConfig.isTlsEnabled());
        return new SchemaManager(connectionManager);
    }

    private String getS3PathPrefix() {
        final String s3UserPathPrefix;
        if (sourceConfig.getS3Prefix() != null && !sourceConfig.getS3Prefix().isBlank()) {
            s3UserPathPrefix = sourceConfig.getS3Prefix();
        } else {
            s3UserPathPrefix = "";
        }

        final String s3PathPrefix;
        if (sourceCoordinator.getPartitionPrefix() != null ) {
            // The prefix will be used in RDS export, which has a limit of 60 characters.
            s3PathPrefix = s3UserPathPrefix + S3_PATH_DELIMITER + IdentifierShortener.shortenIdentifier(sourceCoordinator.getPartitionPrefix(), MAX_SOURCE_IDENTIFIER_LENGTH);
        } else {
            s3PathPrefix = s3UserPathPrefix;
        }
        return s3PathPrefix;
    }


}
