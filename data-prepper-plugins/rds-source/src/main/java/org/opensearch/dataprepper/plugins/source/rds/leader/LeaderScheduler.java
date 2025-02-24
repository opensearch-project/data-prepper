/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds.leader;

import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.rds.RdsSourceConfig;
import org.opensearch.dataprepper.plugins.source.rds.configuration.EngineType;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.ExportPartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.ExportProgressState;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.LeaderProgressState;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.MySqlStreamState;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.PostgresStreamState;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.StreamProgressState;
import org.opensearch.dataprepper.plugins.source.rds.model.BinlogCoordinate;
import org.opensearch.dataprepper.plugins.source.rds.model.DbTableMetadata;
import org.opensearch.dataprepper.plugins.source.rds.schema.MySqlSchemaManager;
import org.opensearch.dataprepper.plugins.source.rds.schema.PostgresSchemaManager;
import org.opensearch.dataprepper.plugins.source.rds.schema.SchemaManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
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
    private final DbTableMetadata dbTableMetadataMetadata;

    private LeaderPartition leaderPartition;
    private StreamPartition streamPartition = null;
    private volatile boolean shutdownRequested = false;

    public LeaderScheduler(final EnhancedSourceCoordinator sourceCoordinator,
                           final RdsSourceConfig sourceConfig,
                           final String s3Prefix,
                           final SchemaManager schemaManager,
                           final DbTableMetadata dbTableMetadataMetadata) {
        this.sourceCoordinator = sourceCoordinator;
        this.sourceConfig = sourceConfig;
        this.s3Prefix = s3Prefix;
        this.schemaManager = schemaManager;
        this.dbTableMetadataMetadata = dbTableMetadataMetadata;
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
                        LOG.info("Performing initialization as LEADER node.");
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

        // Clean up publication and replication slot for Postgres
        if (streamPartition != null) {
            streamPartition.getProgressState().ifPresent(progressState -> {
                if (EngineType.fromString(progressState.getEngineType()).isPostgres()) {
                    final PostgresStreamState postgresStreamState = progressState.getPostgresStreamState();
                    final String publicationName = postgresStreamState.getPublicationName();
                    final String replicationSlotName = postgresStreamState.getReplicationSlotName();
                    LOG.info("Cleaned up logical replication slot {} and publication {}",
                            replicationSlotName, publicationName);
                    ((PostgresSchemaManager) schemaManager).deleteLogicalReplicationSlot(
                            publicationName, replicationSlotName);
                }
            });
        }
    }

    private void init() {
        LOG.info("Initializing RDS source service...");

        // Create a Global state in the coordination table for rds cluster/instance information.
        // Global State here is designed to be able to read whenever needed
        // So that the jobs can refer to the configuration.
        sourceCoordinator.createPartition(new GlobalState(sourceConfig.getDbIdentifier(), dbTableMetadataMetadata.toMap()));
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
        progressState.setEngineType(sourceConfig.getEngine().toString());
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
                        fullTableName -> schemaManager.getPrimaryKeys(fullTableName)
                ));
    }

    private void createStreamPartition(RdsSourceConfig sourceConfig) {
        final StreamProgressState progressState = new StreamProgressState();
        progressState.setEngineType(sourceConfig.getEngine().toString());
        progressState.setWaitForExport(sourceConfig.isExportEnabled());
        progressState.setPrimaryKeyMap(getPrimaryKeyMap());
        if (sourceConfig.getEngine().isMySql()) {
            final MySqlStreamState mySqlStreamState = new MySqlStreamState();
            getCurrentBinlogPosition().ifPresent(mySqlStreamState::setCurrentPosition);
            mySqlStreamState.setForeignKeyRelations(((MySqlSchemaManager)schemaManager).getForeignKeyRelations(sourceConfig.getTableNames()));
            progressState.setMySqlStreamState(mySqlStreamState);
        } else {
            // Postgres
            // Create replication slot, which will mark the starting point for stream
            final String publicationName = generatePublicationName();
            final String slotName = generateReplicationSlotName();
            ((PostgresSchemaManager)schemaManager).createLogicalReplicationSlot(sourceConfig.getTableNames(), publicationName, slotName);
            final PostgresStreamState postgresStreamState = new PostgresStreamState();
            postgresStreamState.setPublicationName(publicationName);
            postgresStreamState.setReplicationSlotName(slotName);
            progressState.setPostgresStreamState(postgresStreamState);
        }
        streamPartition = new StreamPartition(sourceConfig.getDbIdentifier(), progressState);
        sourceCoordinator.createPartition(streamPartition);
    }

    private Optional<BinlogCoordinate> getCurrentBinlogPosition() {
        Optional<BinlogCoordinate> binlogCoordinate = ((MySqlSchemaManager)schemaManager).getCurrentBinaryLogPosition();
        LOG.debug("Current binlog position: {}", binlogCoordinate.orElse(null));
        return binlogCoordinate;
    }

    private String generatePublicationName() {
        return "data_prepper_publication_" + UUID.randomUUID().toString().substring(0, 8);
    }

    private String generateReplicationSlotName() {
        return "data_prepper_slot_" + UUID.randomUUID().toString().substring(0, 8);
    }
}
