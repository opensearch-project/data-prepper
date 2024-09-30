package org.opensearch.dataprepper.plugins.source.saas.crawler.coordination;

import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.saas.crawler.base.BaseSaasSourcePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

public class LeaderScheduler implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(LeaderScheduler.class);

    /**
     * Default duration to extend the timeout of lease
     */
    private static final int DEFAULT_EXTEND_LEASE_MINUTES = 3;

    /**
     * Default interval to run lease check and shard discovery
     */
    private static final Duration DEFAULT_LEASE_INTERVAL = Duration.ofMinutes(1);

    private final EnhancedSourceCoordinator coordinator;

    private final Duration leaseInterval;

    private LeaderPartition leaderPartition;

    private List<String> streamArns;

    private BaseSaasSourcePlugin sourcePlugin;

    public LeaderScheduler(EnhancedSourceCoordinator coordinator, BaseSaasSourcePlugin sourcePlugin) {
        this(coordinator, DEFAULT_LEASE_INTERVAL);
        this.sourcePlugin = sourcePlugin;
    }

    LeaderScheduler(EnhancedSourceCoordinator coordinator,
                    Duration leaseInterval) {
        this.coordinator = coordinator;
        this.leaseInterval = leaseInterval;
    }

    @Override
    public void run() {
        LOG.debug("Starting Leader Scheduler for initialization and shard discovery");

        while (!Thread.currentThread().isInterrupted()) {
            try {
                // Try to acquire the lease if not owned.
                if (leaderPartition == null) {
                    final Optional<EnhancedSourcePartition> sourcePartition = coordinator.acquireAvailablePartition(LeaderPartition.PARTITION_TYPE);
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
                        LOG.debug("The service is not been initialized");
                        sourcePlugin.init(leaderPartition);
                    } else {
                        // The initialization process will populate that value, otherwise, get from state
                        if (streamArns == null) {
                            streamArns = leaderProgressState.getStreamArns();
                        }
                    }
                }

            } catch (Exception e) {
                LOG.error("Exception occurred in primary scheduling loop", e);
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
        LOG.info("Try to initialize DynamoDB service");


        LOG.debug("Update initialization state");
        LeaderProgressState leaderProgressState = leaderPartition.getProgressState().get();
        leaderProgressState.setStreamArns(streamArns);
        leaderProgressState.setInitialized(true);
    }

}
