package org.opensearch.dataprepper.plugins.source.saas.crawler.coordination.scheduler;

import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.saas.crawler.base.Crawler;
import org.opensearch.dataprepper.plugins.source.saas.crawler.base.SaasSourcePlugin;
import org.opensearch.dataprepper.plugins.source.saas.crawler.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.source.saas.crawler.coordination.state.LeaderProgressState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
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

    private final SaasSourcePlugin sourcePlugin;

    private final Crawler crawler;

    public LeaderScheduler(EnhancedSourceCoordinator coordinator,
                           SaasSourcePlugin sourcePlugin,
                           Crawler crawler) {
        this.coordinator = coordinator;
        this.leaseInterval =  DEFAULT_LEASE_INTERVAL;
        this.sourcePlugin = sourcePlugin;
        this.crawler = crawler;
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
                // May want to quit this scheduler if we don't want to monitor future changes
                if (leaderPartition != null) {
                    long lastPollTime = 0L;
                    LeaderProgressState leaderProgressState = leaderPartition.getProgressState().get();
                    if (!leaderProgressState.isInitialized()) {
                        LOG.debug("The service is not been initialized");
                        init();
                    } else {
                        lastPollTime = leaderProgressState.getLastPollTime();
                    }

                    //Start crawling and create child partitions
                    long updatedPollTime = crawler.crawl(lastPollTime, coordinator);
                    leaderProgressState.setLastPollTime(updatedPollTime);
                    leaderPartition.setLeaderProgressState(leaderProgressState);
                    coordinator.saveProgressStateForPartition(leaderPartition, null);
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
                    Thread.currentThread().interrupt();
                }
            }
        }
        // Should Stop
        LOG.warn("Quitting Leader Scheduler");
        if (leaderPartition != null) {
            coordinator.giveUpPartition(leaderPartition);
        }
    }

    private LeaderProgressState init() {
        LOG.info("Initializing Leader Scheduler");
        LeaderProgressState leaderProgressState = leaderPartition.getProgressState().get();
        leaderProgressState.setLastPollTime(0L);
        leaderProgressState.setInitialized(true);
        return leaderProgressState;
    }

}
