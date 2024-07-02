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
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.ExportProgressState;
import org.opensearch.dataprepper.plugins.source.rds.coordination.state.LeaderProgressState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

public class LeaderScheduler implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(LeaderScheduler.class);
    private static final int DEFAULT_EXTEND_LEASE_MINUTES = 3;
    private static final Duration DEFAULT_LEASE_INTERVAL = Duration.ofMinutes(1);
    private final EnhancedSourceCoordinator sourceCoordinator;
    private final RdsSourceConfig sourceConfig;

    private LeaderPartition leaderPartition;
    private volatile boolean shutdownRequested = false;

    public LeaderScheduler(final EnhancedSourceCoordinator sourceCoordinator, final RdsSourceConfig sourceConfig) {
        this.sourceCoordinator = sourceCoordinator;
        this.sourceConfig = sourceConfig;
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

        // Create a Global state in the coordination table for the configuration.
        // Global State here is designed to be able to read whenever needed
        // So that the jobs can refer to the configuration.
        sourceCoordinator.createPartition(new GlobalState(sourceConfig.getDbIdentifier(), null));

        if (sourceConfig.isExportEnabled()) {
            Instant startTime = Instant.now();
            LOG.debug("Export is enabled. Creating export partition in the source coordination store.");
            createExportPartition(sourceConfig, startTime);
        }

        LOG.debug("Update initialization state");
        LeaderProgressState leaderProgressState = leaderPartition.getProgressState().get();
        leaderProgressState.setInitialized(true);
    }

    private void createExportPartition(RdsSourceConfig sourceConfig, Instant exportTime) {
        ExportProgressState progressState = new ExportProgressState();
        progressState.setIamRoleArn(sourceConfig.getAwsAuthenticationConfig().getAwsStsRoleArn());
        progressState.setBucket(sourceConfig.getS3Bucket());
        progressState.setPrefix(sourceConfig.getS3Prefix());
        progressState.setTables(sourceConfig.getTableNames());
        progressState.setKmsKeyId(sourceConfig.getExport().getKmsKeyId());
        progressState.setExportTime(exportTime.toString());
        ExportPartition exportPartition = new ExportPartition(sourceConfig.getDbIdentifier(), sourceConfig.isCluster(), progressState);
        sourceCoordinator.createPartition(exportPartition);
    }

}
