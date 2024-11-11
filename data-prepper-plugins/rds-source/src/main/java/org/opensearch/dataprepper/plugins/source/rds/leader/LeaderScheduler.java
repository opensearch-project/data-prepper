/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.leader;

import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.ExportPartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.ExportProgressState;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.LeaderProgressState;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.StreamProgressState;
import org.opensearch.dataprepper.plugins.source.rds.model.BinlogCoordinate;
import org.opensearch.dataprepper.plugins.source.rds.model.DbMetadata;
import org.opensearch.dataprepper.plugins.source.rds.schema.SchemaManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.opensearch.dataprepper.plugins.source.rds.RdsService.S3_PATH_DELIMITER;

public class LeaderScheduler implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(LeaderScheduler.class);
    private static final int DEFAULT_EXTEND_LEASE_MINUTES = 3;
    private static final Duration DEFAULT_LEASE_INTERVAL = Duration.ofMinutes(1);
    private static final String S3_EXPORT_PREFIX = "rds";
    private final EnhancedSourceCoordinator sourceCoordinator;
    private final RdsSourceConfig sourceConfig;
    private final String s3Prefix;
    private final SchemaManager schemaManager;
    private final DbMetadata dbMetadata;

    private LeaderPartition leaderPartition;
    private volatile boolean shutdownRequested = false;

    public LeaderScheduler(final EnhancedSourceCoordinator sourceCoordinator,
                           final RdsSourceConfig sourceConfig,
                           final String s3Prefix,
                           final SchemaManager schemaManager,
                           final DbMetadata dbMetadata) {
        this.sourceCoordinator = sourceCoordinator;
        this.sourceConfig = sourceConfig;
        this.s3Prefix = s3Prefix;
        this.schemaManager = schemaManager;
        this.dbMetadata = dbMetadata;
    }

    @Override
    public void run() {
        LOG.info("Starting Leader Scheduler for initialization.");

        while (!shutdownRequested && !Thread.currentThread().isInterrupted()) {
            try {
                // Try to acquire the lease if not owned
                if (leaderPartition == null) {
                    final Optional<EnhancedSourcePartition> sourcePartition = sourceCoordinator.acquireAvailablePartition(LeaderPartition.PARTITION_TYPE);
                    if (sourcePartition.isPresent()) {
                        LOG.info("Running as a LEADER node.");
                        leaderPartition = (LeaderPartition) sourcePartition.get();
                    }
                }

                // Once owned, run Normal LEADER node process
                if (leaderPartition != null) {
                    LeaderProgressState leaderProgressState = leaderPartition.getProgressState().get();
                    if (!leaderProgressState.isInitialized()) {
                        init();
                    }
                }
            } catch (final Exception e) {
                LOG.error("Exception occurred in primary leader scheduling loop", e);
            } finally {
                if (leaderPartition != null) {
                    // Extend the timeout
                    // will always be a leader until shutdown
                    sourceCoordinator.saveProgressStateForPartition(leaderPartition, Duration.ofMinutes(DEFAULT_EXTEND_LEASE_MINUTES));
                }

                try {
                    Thread.sleep(DEFAULT_LEASE_INTERVAL.toMillis());
                } catch (final InterruptedException e) {
                    LOG.info("InterruptedException occurred while waiting in leader scheduling loop.");
                    break;
                }
            }
        }

        // Should stop
        LOG.warn("Quitting Leader Scheduler");
        if (leaderPartition != null) {
            sourceCoordinator.giveUpPartition(leaderPartition);
        }
    }

    public void shutdown() {
        shutdownRequested = true;
    }

    private void init() {
        LOG.info("Initializing RDS source service...");

        // Create a Global state in the coordination table for rds cluster/instance information.
        // Global State here is designed to be able to read whenever needed
        // So that the jobs can refer to the configuration.
        sourceCoordinator.createPartition(new GlobalState(sourceConfig.getDbIdentifier(), dbMetadata.toMap()));
        LOG.debug("Created global state for DB: {}", sourceConfig.getDbIdentifier());

        if (sourceConfig.isExportEnabled()) {
            LOG.debug("Export is enabled. Creating export partition in the source coordination store.");
            createExportPartition(sourceConfig);
        }

        if (sourceConfig.isStreamEnabled()) {
            LOG.debug("Stream is enabled. Creating stream partition in the source coordination store.");
            createStreamPartition(sourceConfig);
        }

        LOG.debug("Update initialization state");
        LeaderProgressState leaderProgressState = leaderPartition.getProgressState().get();
        leaderProgressState.setInitialized(true);
    }

    private void createExportPartition(RdsSourceConfig sourceConfig) {
        ExportProgressState progressState = new ExportProgressState();
        progressState.setIamRoleArn(sourceConfig.getExport().getIamRoleArn());
        progressState.setBucket(sourceConfig.getS3Bucket());
        // This prefix is for data exported from RDS
        progressState.setPrefix(getS3PrefixForExport(s3Prefix));
        progressState.setTables(sourceConfig.getTableNames());
        progressState.setKmsKeyId(sourceConfig.getExport().getKmsKeyId());
        progressState.setPrimaryKeyMap(getPrimaryKeyMap());
        ExportPartition exportPartition = new ExportPartition(sourceConfig.getDbIdentifier(), sourceConfig.isCluster(), progressState);
        sourceCoordinator.createPartition(exportPartition);
    }

    private String getS3PrefixForExport(final String givenS3Prefix) {
        return givenS3Prefix + S3_PATH_DELIMITER + S3_EXPORT_PREFIX;
    }

    private Map<String, List<String>> getPrimaryKeyMap() {
        return sourceConfig.getTableNames().stream()
                .collect(Collectors.toMap(
                        fullTableName -> fullTableName,
                        fullTableName -> schemaManager.getPrimaryKeys(fullTableName.split("\\.")[0], fullTableName.split("\\.")[1])
                ));
    }

    private void createStreamPartition(RdsSourceConfig sourceConfig) {
        final StreamProgressState progressState = new StreamProgressState();
        progressState.setWaitForExport(sourceConfig.isExportEnabled());
        getCurrentBinlogPosition().ifPresent(progressState::setCurrentPosition);
        progressState.setForeignKeyRelations(schemaManager.getForeignKeyRelations(sourceConfig.getTableNames()));
        StreamPartition streamPartition = new StreamPartition(sourceConfig.getDbIdentifier(), progressState);
        sourceCoordinator.createPartition(streamPartition);
    }

    private Optional<BinlogCoordinate> getCurrentBinlogPosition() {
        Optional<BinlogCoordinate> binlogCoordinate = schemaManager.getCurrentBinaryLogPosition();
        LOG.debug("Current binlog position: {}", binlogCoordinate.orElse(null));
        return binlogCoordinate;
    }
}
