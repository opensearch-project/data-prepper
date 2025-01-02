/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.neptune.leader;

import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.neptune.configuration.NeptuneSourceConfig;
import org.opensearch.dataprepper.plugins.source.neptune.coordination.partition.GlobalState;
import org.opensearch.dataprepper.plugins.source.neptune.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.source.neptune.coordination.partition.S3FolderPartition;
import org.opensearch.dataprepper.plugins.source.neptune.coordination.partition.StreamPartition;
import org.opensearch.dataprepper.plugins.source.neptune.coordination.state.LeaderProgressState;
import org.opensearch.dataprepper.plugins.source.neptune.coordination.state.StreamProgressState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class LeaderScheduler implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(LeaderScheduler.class);

    private static final int DEFAULT_PARTITION_COUNT = 100;

    /**
     * Default duration to extend the timeout of lease
     */
    static final int DEFAULT_EXTEND_LEASE_MINUTES = 3;

    /**
     * Default interval to run lease check and shard discovery
     */
    private static final Duration DEFAULT_LEASE_INTERVAL = Duration.ofMinutes(1);

    private final NeptuneSourceConfig sourceConfig;

    private final EnhancedSourceCoordinator coordinator;
    private final String s3PathPrefix;

    private final Duration leaseInterval;

    private LeaderPartition leaderPartition;

    public LeaderScheduler(final EnhancedSourceCoordinator coordinator, final NeptuneSourceConfig sourceConfig, final String s3PathPrefix) {
        this(coordinator, sourceConfig, s3PathPrefix, DEFAULT_LEASE_INTERVAL);
    }

    LeaderScheduler(final EnhancedSourceCoordinator coordinator,
                    final NeptuneSourceConfig sourceConfig,
                    final String s3PathPrefix,
                    final Duration leaseInterval) {
        this.sourceConfig = sourceConfig;
        this.coordinator = coordinator;
        checkArgument(Objects.nonNull(s3PathPrefix), "S3 path prefix must not be null");
        this.s3PathPrefix = s3PathPrefix;
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
        LOG.info("Try to initialize Neptune Leader Partition");

        coordinator.createPartition(new GlobalState("neptune", null));

        final Instant startTime = Instant.now();
        final String s3Prefix = s3PathPrefix + "neptune";
        createS3Partition(sourceConfig.getS3Bucket(), sourceConfig.getS3Region(), s3Prefix);

        if (sourceConfig.isStream()) {
            createStreamPartition(startTime);
        }

        LOG.debug("Update initialization state");
        LeaderProgressState leaderProgressState = leaderPartition.getProgressState().get();
        leaderProgressState.setInitialized(true);
    }

    /**
     * Create a partition for a S3 partition creator job in the coordination table.
     */
    private void createS3Partition(final String s3Bucket, final String s3Region, final String s3PathPrefix) {
        LOG.info("Creating s3 folder global partition...");
        // TODO: change to read partition count from config
        coordinator.createPartition(new S3FolderPartition(s3Bucket, s3PathPrefix, s3Region, DEFAULT_PARTITION_COUNT));
    }

    /**
     * Create a partition for a stream job in the coordination table.
     *
     * @param streamTime the start time for change events, any change events with creation datetime before this should be ignored.
     */
    private void createStreamPartition(final Instant streamTime) {
        LOG.info("Creating stream global partition...");
        final StreamProgressState streamProgressState = new StreamProgressState();
        streamProgressState.setStartTime(streamTime.toEpochMilli());
        streamProgressState.setLastUpdateTimestamp(Instant.now().toEpochMilli());
        coordinator.createPartition(new StreamPartition(streamProgressState));
    }
}
