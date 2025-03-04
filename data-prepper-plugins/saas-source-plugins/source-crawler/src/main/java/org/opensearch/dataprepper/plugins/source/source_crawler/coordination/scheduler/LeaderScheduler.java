package org.opensearch.dataprepper.plugins.source.source_crawler.coordination.scheduler;

import lombok.Setter;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;
import org.opensearch.dataprepper.plugins.source.source_crawler.base.Crawler;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.partition.LeaderPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;

public class LeaderScheduler implements Runnable {

    /**
     * Default duration to extend the timeout of lease
     */
    public static final Duration DEFAULT_EXTEND_LEASE_MINUTES = Duration.ofMinutes(3);
    private static final Logger LOG = LoggerFactory.getLogger(LeaderScheduler.class);
    /**
     * Default interval to run lease check and shard discovery
     */
    private static final Duration DEFAULT_LEASE_INTERVAL = Duration.ofMinutes(1);
    @Setter
    private Duration leaseInterval;
    private LeaderPartition leaderPartition;
    private final EnhancedSourceCoordinator coordinator;
    private final Crawler crawler;
    private final int batchSize;

    public LeaderScheduler(EnhancedSourceCoordinator coordinator,
                           Crawler crawler,
                           int batchSize) {
        this.coordinator = coordinator;
        this.leaseInterval = DEFAULT_LEASE_INTERVAL;
        this.crawler = crawler;
        this.batchSize = batchSize;
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
                    //Start crawling, create child partitions and also continue to update leader partition state
                    crawler.crawl(leaderPartition, coordinator, batchSize);
                }

            } catch (Exception e) {
                LOG.error("Exception occurred in primary scheduling loop", e);
            } finally {
                if (leaderPartition != null) {
                    // Extend the timeout
                    // will always be a leader until shutdown
                    try {
                        coordinator.saveProgressStateForPartition(leaderPartition, DEFAULT_EXTEND_LEASE_MINUTES);
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
