package org.opensearch.dataprepper.plugins.source.source_crawler.base;

import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.partition.LeaderPartition;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.partition.SaasSourcePartition;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.CrowdStrikeLeaderProgressState;
import org.opensearch.dataprepper.plugins.source.source_crawler.coordination.state.CrowdStrikeWorkerProgressState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.inject.Named;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import static org.opensearch.dataprepper.plugins.source.source_crawler.coordination.scheduler.LeaderScheduler.DEFAULT_EXTEND_LEASE_MINUTES;

/**
 * A crawler implementation that processes data in specific time slices.
 * This class provides functionality to crawl and collect data within defined time intervals,
 * helping to manage resource utilization and rate limiting.
 */
@Named
public class TimeSliceCrawler implements Crawler {
    private static final Logger log = LoggerFactory.getLogger(TimeSliceCrawler.class);
    private static final String TIME_SLICE_WORKER_PARTITIONS_CREATED = "TimeSliceWorkerPartitionsCreated";
    private final CrawlerClient client;
    private final Counter partitionsCreatedCounter;
    private static final String LAST_UPDATED_KEY = "last_updated|";

    public TimeSliceCrawler(CrawlerClient client, PluginMetrics pluginMetrics) {
        this.client = client;
        this.partitionsCreatedCounter = pluginMetrics.counter(TIME_SLICE_WORKER_PARTITIONS_CREATED);
    }

    /**
     * Main crawling logic for CrowdStrike using time-based partitioning.
     */
    @Override
    public Instant crawl(LeaderPartition leaderPartition, EnhancedSourceCoordinator coordinator) {
        Instant latestModifiedTime =  Instant.now();
        double startCount = partitionsCreatedCounter.count();
        createPartitionForCrawling(leaderPartition, coordinator, latestModifiedTime);
        double partitionsInThisCrawl = partitionsCreatedCounter.count() - startCount;
        log.info("Total partitions created in this crawl: {}", partitionsInThisCrawl);
        return latestModifiedTime;
    }

    @Override
    public void executePartition(SaasWorkerProgressState state, Buffer buffer, AcknowledgementSet acknowledgementSet) {
        client.executePartition(state, buffer, acknowledgementSet);
    }

    private void createPartitionForCrawling(LeaderPartition leaderPartition, EnhancedSourceCoordinator coordinator, Instant latestModifiedTime) {
        CrowdStrikeLeaderProgressState leaderProgressState = (CrowdStrikeLeaderProgressState) leaderPartition.getProgressState().get();
        if (leaderProgressState.getRemainingDays() == 0) {
            createPartitionForIncrementalSync(leaderPartition, coordinator, latestModifiedTime);
        } else {
            createPartitionForHistoricalPull(leaderPartition, coordinator, latestModifiedTime);
        }
    }

    /**
     * Updates the leader progress state with the latest poll timestamp and remaining days.
     * This method also persists the updated state in the source coordinator.
     */
    private void updateLeaderProgressState(LeaderPartition leaderPartition, int remainingDays, Instant updatedPollTime, EnhancedSourceCoordinator coordinator) {
        CrowdStrikeLeaderProgressState state = (CrowdStrikeLeaderProgressState) leaderPartition.getProgressState().get();
        state.setRemainingDays(remainingDays);
        state.setLastPollTime(updatedPollTime);
        leaderPartition.setLeaderProgressState(state);
        coordinator.saveProgressStateForPartition(leaderPartition, DEFAULT_EXTEND_LEASE_MINUTES);
    }

    /**
     * Creates a single incremental sync partition from the last poll time to now
     * and updates the leader progress state accordingly.
     */
    private void createPartitionForIncrementalSync(LeaderPartition leaderPartition, EnhancedSourceCoordinator coordinator, Instant latestModifiedTime) {
        LeaderProgressState leaderProgressState = leaderPartition.getProgressState().get();
        createWorkerPartition(leaderProgressState.getLastPollTime(), latestModifiedTime, coordinator);
        updateLeaderProgressState(leaderPartition, 0, latestModifiedTime, coordinator);
    }

    /**
     * Creates time-based partitions for a historical pull using the remainingDays value.
     * Partitions are aligned to UTC day boundaries.
     */
    private void createPartitionForHistoricalPull(LeaderPartition leaderPartition, EnhancedSourceCoordinator coordinator, Instant latestModifiedTime) {
        CrowdStrikeLeaderProgressState leaderProgressState = (CrowdStrikeLeaderProgressState) leaderPartition.getProgressState().get();
        int remainingDays = leaderProgressState.getRemainingDays();
        Instant initialDate = leaderProgressState.getLastPollTime();
        Instant todayUtc = initialDate.truncatedTo(ChronoUnit.DAYS);
        for(int i = remainingDays; i > 0; i--) {
            Instant startDate = todayUtc.minus(Duration.ofDays(i));
            createWorkerPartition(startDate, startDate.plus(Duration.ofDays(1)), coordinator);
        }
        // Create a final partition from today's midnight to now
        createWorkerPartition(todayUtc, latestModifiedTime, coordinator);
        updateLeaderProgressState(leaderPartition, 0, latestModifiedTime, coordinator);
    }


    /**
     * Creates a new worker partition between given time boundaries and registers it with the coordinator.
     */
    void createWorkerPartition(Instant startTime, Instant endTime, EnhancedSourceCoordinator coordinator) {
        CrowdStrikeWorkerProgressState workerState = new CrowdStrikeWorkerProgressState();
        workerState.setStartTime(startTime);
        workerState.setEndTime(endTime);
        SaasSourcePartition partition = new SaasSourcePartition(workerState, LAST_UPDATED_KEY + UUID.randomUUID());
        coordinator.createPartition(partition);
        partitionsCreatedCounter.increment();
        log.info("Created partition from {} to {}", startTime, endTime);
    }
}
