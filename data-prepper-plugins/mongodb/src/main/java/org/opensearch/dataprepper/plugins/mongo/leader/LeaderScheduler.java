/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.leader;

import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.ExportPartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.mongo.coordination.state.ExportProgressState;
import org.opensearch.dataprepper.plugins.mongo.coordination.state.LeaderProgressState;
import org.opensearch.dataprepper.plugins.mongo.coordination.state.StreamProgressState;
import org.opensearch.dataprepper.plugins.mongo.configuration.CollectionConfig;
import org.opensearch.dataprepper.plugins.mongo.model.ExportLoadStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

public class LeaderScheduler implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(LeaderScheduler.class);
    public static final String EXPORT_PREFIX = "EXPORT-";

    /**
     * Default duration to extend the timeout of lease
     */
    static final int DEFAULT_EXTEND_LEASE_MINUTES = 3;

    /**
     * Default interval to run lease check and shard discovery
     */
    private static final Duration DEFAULT_LEASE_INTERVAL = Duration.ofMinutes(1);

    private final List<CollectionConfig> collectionConfigs;

    private final EnhancedSourceCoordinator coordinator;

    private final Duration leaseInterval;

    private LeaderPartition leaderPartition;

    public LeaderScheduler(EnhancedSourceCoordinator coordinator, List<CollectionConfig> collectionConfigs) {
        this(coordinator, collectionConfigs, DEFAULT_LEASE_INTERVAL);
    }

    LeaderScheduler(EnhancedSourceCoordinator coordinator,
                    List<CollectionConfig> collectionConfigs,
                    Duration leaseInterval) {
        this.collectionConfigs = collectionConfigs;
        this.coordinator = coordinator;
        this.leaseInterval = leaseInterval;
    }

    @Override
    public void run() {
        LOG.info("Starting Leader Scheduler for initialization and stream discovery");

        while (!Thread.currentThread().isInterrupted()) {
            try {
                // Try to acquire the lease if not owned.
                if (leaderPartition == null) {
                    final Optional<EnhancedSourcePartition> sourcePartition = coordinator.acquireAvailablePartition(LeaderPartition.PARTITION_TYPE);
                    LOG.info("Leader partition {}", sourcePartition);
                    if (sourcePartition.isPresent()) {
                        LOG.info("Running as a LEADER node");
                        leaderPartition = (LeaderPartition) sourcePartition.get();
                    }
                }
                // Once owned, run Normal LEADER node process.
                // May want to quit this scheduler if streaming is not required
                if (leaderPartition != null) {
                    LeaderProgressState leaderProgressState = leaderPartition.getProgressState().get();
                    if (!leaderProgressState.isInitialized()) {
                        LOG.info("The service is not been initialized");
                        init();
                    }

                }

            } catch (final Exception e) {
                LOG.error("Exception occurred in primary leader scheduling loop", e);
            } finally {
                if(leaderPartition != null) {
                    // Extend the timeout
                    // will always be a leader until shutdown
                    coordinator.saveProgressStateForPartition(leaderPartition, Duration.ofMinutes(DEFAULT_EXTEND_LEASE_MINUTES));
                }
                try {
                    Thread.sleep(leaseInterval.toMillis());
                } catch (final InterruptedException e) {
                    LOG.info("InterruptedException occurred");
                    break;
                }
            }
        }
        // Should Stop
        LOG.warn("Quitting Leader Scheduler");
        if (leaderPartition != null) {
            coordinator.giveUpPartition(leaderPartition);
        }
    }


    private void init() {
        LOG.info("Try to initialize DocumentDB Leader Partition");

        collectionConfigs.forEach(collectionConfig -> {
            // Create a Global state in the coordination table for the configuration.
            // Global State here is designed to be able to read whenever needed
            // So that the jobs can refer to the configuration.
            coordinator.createPartition(new GlobalState(collectionConfig.getCollection(), null));

            final Instant startTime = Instant.now();
            final boolean exportRequired = collectionConfig.isExportRequired();
            LOG.info("Ingestion mode {} for Collection {}", collectionConfig.getIngestionMode(), collectionConfig.getCollection());
            if (exportRequired) {
                createExportPartition(collectionConfig, startTime);
                createExportGlobalState(collectionConfig);
            }

            if (collectionConfig.isStreamRequired()) {
                createStreamPartition(collectionConfig, startTime, exportRequired);
            }

        });

        LOG.debug("Update initialization state");
        LeaderProgressState leaderProgressState = leaderPartition.getProgressState().get();
        leaderProgressState.setInitialized(true);
    }


    /**
     * Create a partition for a stream job in the coordination table.
     *
     * @param collectionConfig  collection configuration object containing collection details
     * @param streamTime the start time for change events, any change events with creation datetime before this should be ignored.
     */
    private void createStreamPartition(final CollectionConfig collectionConfig, final Instant streamTime, final boolean waitForExport) {
        LOG.info("Creating stream global partition: {}", collectionConfig.getCollection());
        final StreamProgressState streamProgressState = new StreamProgressState();
        streamProgressState.setWaitForExport(waitForExport);
        streamProgressState.setStartTime(streamTime.toEpochMilli());
        streamProgressState.setLastUpdateTimestamp(Instant.now().toEpochMilli());
        coordinator.createPartition(new StreamPartition(collectionConfig.getCollection(), streamProgressState));
    }

    /**
     * Create a partition for an export job in the coordination table.
     *
     * @param collectionConfig  collection configuration object containing collection details
     * @param exportTime the start time for Export
     */
    private void createExportPartition(final CollectionConfig collectionConfig, final Instant exportTime) {
        LOG.info("Creating export global partition for collection: {}", collectionConfig.getCollection());
        final ExportProgressState exportProgressState = new ExportProgressState();
        exportProgressState.setCollectionName(collectionConfig.getCollectionName());
        exportProgressState.setDatabaseName(collectionConfig.getDatabaseName());
        exportProgressState.setExportTime(exportTime.toString()); // information purpose
        final ExportPartition exportPartition = new ExportPartition(collectionConfig.getCollection(),
                collectionConfig.getExportConfig().getItemsPerPartition(), exportTime, exportProgressState);
        coordinator.createPartition(exportPartition);
    }

    private void createExportGlobalState(final CollectionConfig collectionConfig) {
        final ExportLoadStatus exportLoadStatus = new ExportLoadStatus(0, 0, 0, Instant.now().toEpochMilli());
        coordinator.createPartition(
                new GlobalState(EXPORT_PREFIX + collectionConfig.getCollection(), exportLoadStatus.toMap()));
    }
}
