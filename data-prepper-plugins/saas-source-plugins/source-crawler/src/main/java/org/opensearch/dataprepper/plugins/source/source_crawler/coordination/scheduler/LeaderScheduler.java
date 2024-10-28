package org.opensearch.dataprepper.plugins.source.source_crawler.coordination.scheduler;

import lombok.Setter;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.Crawler;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.CrawlerSourcePlugin;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.LeaderProgressState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
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
    private final CrawlerSourcePlugin sourcePlugin;
    private final Crawler crawler;
    @Setter
    private Duration leaseInterval;
    private LeaderPartition leaderPartition;

    public LeaderScheduler(EnhancedSourceCoordinator coordinator,
                           CrawlerSourcePlugin sourcePlugin,
                           Crawler crawler) {
        this.coordinator = coordinator;
        this.leaseInterval = DEFAULT_LEASE_INTERVAL;
        this.sourcePlugin = sourcePlugin;
        this.crawler = crawler;
    }

    @Override
    public void run() {
        LOG.debug("Starting Leader Scheduler for initialization and source partition discovery");

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
                    LeaderProgressState leaderProgressState = leaderPartition.getProgressState().get();
                    Instant lastPollTime = leaderProgressState.getLastPollTime();

                    //Start crawling and create child partitions
                    Instant updatedPollTime = crawler.crawl(lastPollTime, coordinator);
                    leaderProgressState.setLastPollTime(updatedPollTime);
                    leaderPartition.setLeaderProgressState(leaderProgressState);
                    coordinator.saveProgressStateForPartition(leaderPartition, null);
                }

            } catch (Exception e) {
                LOG.error("Exception occurred in primary scheduling loop", e);
            } finally {
                if (leaderPartition != null) {
                    // Extend the timeout
                    // will always be a leader until shutdown
                    try {
                        coordinator.saveProgressStateForPartition(leaderPartition, Duration.ofMinutes(DEFAULT_EXTEND_LEASE_MINUTES));
                    } catch (final Exception e) {
                        LOG.error("Failed to save Leader partition state. This process will retry.");
                    }
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

}
